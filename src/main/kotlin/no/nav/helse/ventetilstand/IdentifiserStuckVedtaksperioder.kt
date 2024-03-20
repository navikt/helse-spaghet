package no.nav.helse.ventetilstand

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.ObjectMapper
import no.nav.helse.objectMapper
import no.nav.helse.rapids_rivers.*
import no.nav.helse.ventetilstand.Slack.sendPåSlack
import org.slf4j.LoggerFactory
import org.slf4j.event.Level.ERROR
import org.slf4j.event.Level.INFO
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.util.*
import javax.sql.DataSource
import kotlin.time.DurationUnit.SECONDS
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

internal class IdentifiserStuckVedtaksperioder (
    rapidsConnection: RapidsConnection,
    dataSource: DataSource
): River.PacketListener {
    private val dao = VedtaksperiodeVentetilstandDao(dataSource)

    init {
        River(rapidsConnection).apply {
            validate {
                it.demandValue("@event_name", "identifiser_stuck_vedtaksperioder")
                it.requireKey("system_participating_services")
            }
        }.register(this)
        River(rapidsConnection).apply {
            validate {
                it.demandValue("@event_name", "halv_time")
                it.demand("time") { time ->
                    check(time.asInt() in setOf(8, 13))
                }
                it.demandValue("minutt", 30)
                it.rejectValues("ukedag", listOf("SATURDAY", "SUNDAY"))
                it.requireKey("system_participating_services")
            }
        }.register(this)
    }

    @ExperimentalTime
    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        try {
            val (stuck, tidsbruk) = measureTimedValue { dao.stuck() }
            "Sjekket om vedtaksperioder er stuck på grunn av '${packet.eventname}'. Det tok ${tidsbruk.toString(SECONDS)}".let {
                if (tidsbruk.inWholeSeconds > 2) sikkerlogg.error(it) else sikkerlogg.info(it)
            }
            if (stuck.isEmpty()) return ingentingStuck(packet, context)

            val venterPå = stuck
                .groupBy { it.fødselsnummer }
                .mapValues { (_, vedtaksperioder) -> vedtaksperioder.first().venterPå }
                .filterNot {  it.value.skalIkkeMase }
                .takeUnless { it.isEmpty() } ?: return ingentingStuck(packet, context)


            val antall = venterPå.size

            var melding =
                "\n\nDet er ${stuck.size.vedtaksperioder} som ser ut til å være stuck! :helene-redteam:\n" +
                "Fordelt på ${antall.personer}:\n\n"

            var index = 0
            melding += venterPå.entries.take(Maks).joinToString(separator = "\n") { (fnr, vedtaksperiode) ->
                index += 1
                "\t$index) ${vedtaksperiode.kibanaUrl} venter på ${vedtaksperiode.snygg} ${fnr.spannerUrl?.let { "($it)" }}"
            }

            if (antall > Maks) melding += "\n\t... og ${antall - Maks} til.. :melting_face:"

            context.sendPåSlack(packet, ERROR, melding)
        } catch (exception: Exception) {
            sikkerlogg.error("Feil ved identifisering av stuck vedtaksperioder", exception)
        }
    }

    private companion object {
        private val Maks = 50
        private val spurteDuClient = SpurteDuClient()
        private val sikkerlogg = LoggerFactory.getLogger("tjenestekall")
        private val VenterPå.snygg get() = if (hvorfor == null) hva else "$hva fordi $hvorfor"
        private val VenterPå.kibanaUrl get() = "https://logs.adeo.no/app/kibana#/discover?_a=(index:'tjenestekall-*',query:(language:lucene,query:'%22${vedtaksperiodeId}%22'))&_g=(time:(from:'${LocalDateTime.now().minusDays(1)}',mode:absolute,to:now))".let { url ->
            "<$url|$vedtaksperiodeId>"
        }
        private val String.spannerUrl get() = spurteDuClient.utveksleUrl("https://spanner.intern.nav.no/person/${this}")?.let { url ->
            "<$url|spannerlink>"
        }

        /** Liste med perioder som er stuck, men bruker kontaktes av saksbehandler for å avvente ny informasjon som kan endre på stuck-situasjonen **/
        private val AVVENTER_MENS_AG_KONTAKTES = emptySet<IkkeStuckLikevel>()
        private val VenterPå.skalIkkeMase get() = IkkeStuckLikevel(vedtaksperiodeId, hva, hvorfor) in AVVENTER_MENS_AG_KONTAKTES
        private data class IkkeStuckLikevel(private val vedtaksperiodeId: UUID, private val hva: String, private val hvorfor: String?)

        private val JsonMessage.eventname get() = get("@event_name").asText()
        private fun ingentingStuck(packet: JsonMessage, context: MessageContext) {
            if (packet.eventname == "identifiser_stuck_vedtaksperioder") return context.sendPåSlack(packet, INFO, "\n\nTa det med ro! Ingenting er stuck! Gå tilbake til det du egentlig skulle gjøre :heart:")
            sikkerlogg.info("Ingen vedtaksperioder er stuck per ${LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS)}")
        }

        private val Int.personer get() = if (this == 1) "én person" else "$this personer"
        private val Int.vedtaksperioder get() = if (this == 1) "én vedtaksperiode" else "$this vedtaksperioder"
    }
}

class SpurteDuClient(private val host: String) {
    constructor() : this(when (System.getenv("NAIS_CLUSTER_NAME")) {
        "prod-gcp" -> "https://spurte-du.intern.nav.no"
        else -> "https://spurte-du.intern.dev.nav.no"
    })
    private companion object {
        private const val tbdgruppeProd = "c0227409-2085-4eb2-b487-c4ba270986a3"
    }

    fun utveksleUrl(url: String) = utveksleSpurteDu(objectMapper, mapOf(
        "url" to url,
        "påkrevdTilgang" to tbdgruppeProd
    ))?.let { path ->
        host + path
    }
    private fun utveksleSpurteDu(objectMapper: ObjectMapper, data: Map<String, String>): String? {
        val httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build()

        val jsonInputString = objectMapper.writeValueAsString(data)

        val request = HttpRequest.newBuilder()
            .uri(URI("http://spurtedu/skjul_meg"))
            .timeout(Duration.ofSeconds(10))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonInputString))
            .build()

        val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())

        return try {
            objectMapper.readTree(response.body()).path("path").asText()
        } catch (err: JsonParseException) {
            null
        }
    }
}