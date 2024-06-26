package no.nav.helse.ventetilstand

import com.github.navikt.tbd_libs.spurtedu.SkjulRequest
import com.github.navikt.tbd_libs.spurtedu.SpurteDuClient
import no.nav.helse.objectMapper
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import no.nav.helse.ventetilstand.Slack.sendPåSlack
import org.slf4j.LoggerFactory
import org.slf4j.event.Level.ERROR
import org.slf4j.event.Level.INFO
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.util.*
import kotlin.time.*

internal class IdentifiserStuckVedtaksperioder(
    rapidsConnection: RapidsConnection,
    private val dao: VedtaksperiodeVentetilstandDao,
    private val spurteDuClient: SpurteDuClient
): River.PacketListener {

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
            if (stuck.isEmpty()) return ingentingStuck(packet, context, tidsbruk)

            val venterPå = stuck
                .groupBy { it.fødselsnummer }
                .mapValues { (_, vedtaksperioder) -> vedtaksperioder.first().venterPå }
                .filterNot {  it.value.skalIkkeMase }
                .takeUnless { it.isEmpty() } ?: return ingentingStuck(packet, context, tidsbruk)


            val antall = venterPå.size

            var melding =
                "\n\nBrukte ${tidsbruk.snygg} på å finne ut at det er ${stuck.size.vedtaksperioder} som ser ut til å være stuck! :helene-redteam:\n" +
                "Fordelt på ${antall.personer}:\n\n"

            var index = 0
            melding += venterPå.entries.take(Maks).joinToString(separator = "\n") { (fnr, vedtaksperiode) ->
                index += 1
                "\t$index) ${vedtaksperiode.kibanaUrl} venter på ${vedtaksperiode.snygg} ${fnr.spannerUrl(spurteDuClient).let { "($it)" }}"
            }

            if (antall > Maks) melding += "\n\t... og ${antall - Maks} til.. :melting_face:"

            context.sendPåSlack(packet, ERROR, melding)
        } catch (exception: Exception) {
            sikkerlogg.error("Feil ved identifisering av stuck vedtaksperioder", exception)
        }
    }

    private companion object {
        private val Maks = 50
        private val sikkerlogg = LoggerFactory.getLogger("tjenestekall")
        private val VenterPå.snygg get() = if (hvorfor == null) hva else "$hva fordi $hvorfor"
        private val VenterPå.kibanaUrl get() = "https://logs.adeo.no/app/kibana#/discover?_a=(index:'tjenestekall-*',query:(language:lucene,query:'%22${vedtaksperiodeId}%22'))&_g=(time:(from:'${LocalDateTime.now().minusDays(1)}',mode:absolute,to:now))".let { url ->
            "<$url|$vedtaksperiodeId>"
        }
        private fun String.spannerUrl(spurteDuClient: SpurteDuClient) = spannerlink(spurteDuClient, this).let { url ->
            "<$url|spannerlink>"
        }

        /** Liste med perioder som er stuck, men bruker kontaktes av saksbehandler for å avvente ny informasjon som kan endre på stuck-situasjonen **/
        private val AVVENTER_MENS_AG_KONTAKTES = emptySet<IkkeStuckLikevel>()
        private val VenterPå.skalIkkeMase get() = IkkeStuckLikevel(vedtaksperiodeId, hva, hvorfor) in AVVENTER_MENS_AG_KONTAKTES
        private data class IkkeStuckLikevel(private val vedtaksperiodeId: UUID, private val hva: String, private val hvorfor: String?)

        private val JsonMessage.eventname get() = get("@event_name").asText()
        private fun ingentingStuck(packet: JsonMessage, context: MessageContext, tidsbruk: Duration) {
            if (packet.eventname == "identifiser_stuck_vedtaksperioder") return context.sendPåSlack(packet, INFO, "\n\nBrukte ${tidsbruk.snygg} på å finne ut at du kan bare ta det helt :musical_keyboard:! Ingenting er stuck! Gå tilbake til det du egentlig skulle gjøre :heart:")
            sikkerlogg.info("Ingen vedtaksperioder er stuck per ${LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS)}")
        }

        private val Int.personer get() = if (this == 1) "én person" else "$this personer"
        private val Int.vedtaksperioder get() = if (this == 1) "én vedtaksperiode" else "$this vedtaksperioder"
        private fun menneskete(ting: String, antall: Int, prefix: String = "") = if (antall <= 0) "" else if (antall == 1) "$prefix$antall $ting" else "$prefix$antall ${ting}er"
        private val Duration.snygg get() = toJavaDuration().let { when {
            it.seconds == 0L -> "under ett sekund"
            it.seconds < 60L -> menneskete("sekund", it.seconds.toInt())
            else -> "${menneskete("minutt", it.toMinutesPart())} ${menneskete("sekund", it.toSecondsPart(), prefix = "og ")}"
        }}
    }
}

private const val tbdgruppeProd = "c0227409-2085-4eb2-b487-c4ba270986a3"
fun spannerlink(spurteDuClient: SpurteDuClient, fnr: String): String {
    val payload = SkjulRequest.SkjulTekstRequest(
        tekst = objectMapper.writeValueAsString(mapOf(
            "ident" to fnr,
            "identtype" to "FNR"
        )),
        påkrevdTilgang = tbdgruppeProd
    )

    val spurteDuLink = spurteDuClient.skjul(payload)
    return "https://spanner.ansatt.nav.no/person/${spurteDuLink.id}"
}
