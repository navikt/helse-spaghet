package no.nav.helse.ventetilstand

import no.nav.helse.rapids_rivers.*
import org.slf4j.LoggerFactory
import org.slf4j.event.Level
import org.slf4j.event.Level.ERROR
import org.slf4j.event.Level.INFO
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
            if (stuck.isEmpty()) return ingentingStuck(packet, context)

            val antallVedtaksperioder = stuck.size
            val venterPå = stuck
                .groupBy { it.fødselsnummer }
                .mapValues { (_, vedtaksperioder) -> vedtaksperioder.first().venterPå }
                .values
                .filterNot { it.ikkeStuckLikevel }
                .takeUnless { it.isEmpty() } ?: return ingentingStuck(packet, context)

            val antallPersoner = venterPå.size

            sikkerlogg.info("Brukte ${tidsbruk.toString(SECONDS)} på å sjekke at $antallVedtaksperioder vedtaksperioder fordelt på $antallPersoner personer er stuck. Varsler på Slack")

            var melding =
                "\n\nDet er ${antallVedtaksperioder.vedtaksperioder} som ser ut til å være stuck! :helene-redteam:\n" +
                "Fordelt på ${antallPersoner.personer}:\n\n"

            venterPå.forEachIndexed { index, it ->
                melding += "\t${index+1}) ${it.kibanaUrl} venter på ${it.snygg}\n"
            }

            melding += if (tidsbruk.inWholeSeconds > 2) "\nDette tok meg ${tidsbruk.toString(SECONDS)} å finne ut av, så nå forventer jeg en innsats også fra deres side :meow_tired:\n\n" else "\n"

            melding += " - Deres erbødig SPaghet :spaghet:"

            context.sendPåSlack(packet, ERROR, melding)
        } catch (exception: Exception) {
            sikkerlogg.error("Feil ved identifisering av stuck vedtaksperioder", exception)
        }
    }

    private companion object {
        private val sikkerlogg = LoggerFactory.getLogger("tjenestekall")
        private val VenterPå.snygg get() = if (hvorfor == null) hva else "$hva fordi $hvorfor"
        private val VenterPå.kibanaUrl get() = "https://logs.adeo.no/app/kibana#/discover?_a=(index:'tjenestekall-*',query:(language:lucene,query:'%22${vedtaksperiodeId}%22'))&_g=(time:(from:'${LocalDateTime.now().minusDays(1)}',mode:absolute,to:now))".let { url ->
            "<$url|$vedtaksperiodeId>"
        }
        /** Liste med perioder vi har manuelt sjekket at ikke er stuck tross at de gir treff på spørringen **/
        private val IKKE_STUCK_LIKEVEL = setOf(
            IkkeStuckLikevel(UUID.fromString("0b8efbb2-8d31-4291-9911-f394a7d9b69a"), "INNTEKTSMELDING", "MANGLER_REFUSJONSOPPLYSNINGER_PÅ_ANDRE_ARBEIDSGIVERE")
        )
        private val VenterPå.ikkeStuckLikevel get() = IkkeStuckLikevel(vedtaksperiodeId, hva, hvorfor) in IKKE_STUCK_LIKEVEL
        private data class IkkeStuckLikevel(private val vedtaksperiodeId: UUID, private val hva: String, private val hvorfor: String?)

        private fun MessageContext.sendPåSlack(packet: JsonMessage, level: Level, melding: String) {
            val slackmelding = JsonMessage.newMessage("slackmelding", mapOf(
                "melding" to melding,
                "level" to level.name,
                "system_participating_services" to packet["system_participating_services"]
            )).toJson()

            publish(slackmelding)
        }
        private val JsonMessage.spoutet get() = get("@event_name").asText() == "identifiser_stuck_vedtaksperioder"
        private fun ingentingStuck(packet: JsonMessage, context: MessageContext) {
            if (packet.spoutet) return context.sendPåSlack(packet, INFO, "\n\nTa det med ro! Ingenting er stuck! Gå tilbake til det du egentlig skulle gjøre :heart:")
            sikkerlogg.info("Ingen vedtaksperioder er stuck per ${LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS)}")
        }

        private val Int.personer get() = if (this == 1) "én person" else "$this personer"
        private val Int.vedtaksperioder get() = if (this == 1) "én vedtaksperiode" else "$this vedtaksperioder"
    }
}