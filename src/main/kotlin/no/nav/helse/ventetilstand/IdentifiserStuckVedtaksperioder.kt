package no.nav.helse.ventetilstand

import no.nav.helse.rapids_rivers.*
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
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
                it.demandValue("time", 8)
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
            if (stuck.isEmpty()) return sikkerlogg.info("Brukte ${tidsbruk.toString(SECONDS)} på å sjekke at ingen vedtaksperioder er stuck")

            val antallVedtaksperioder = stuck.size
            val venterPå = stuck
                .groupBy { it.fødselsnummer }
                .mapValues { (_, vedtaksperioder) -> vedtaksperioder.first().venterPå }
                .values
                .filterNot { it.ikkeStuckLikevel }
                .takeUnless { it.isEmpty() } ?: return sikkerlogg.info("Brukte ${tidsbruk.toString(SECONDS)} på å sjekke at ingen vedtaksperioder er stuck")

            val antallPersoner = venterPå.size

            sikkerlogg.info("Brukte ${tidsbruk.toString(SECONDS)} på å sjekke at $antallVedtaksperioder vedtaksperioder fordelt på $antallPersoner personer er stuck. Varsler på Slack")

            var melding =
                "\nDet er vedtaksperioder som ser ut til å være stuck! :helene-redteam:\n" +
                "Totalt $antallVedtaksperioder vedtaksperioder fordelt på $antallPersoner personer.\n" +
                "Vedtaksperiodene det ventes på per person:\n\n"

            venterPå.forEachIndexed { index, it ->
                melding += "\t${index+1}) ${it.kibanaUrl} venter på ${it.snygg}\n"
            }

            if (tidsbruk.inWholeSeconds > 2) melding += "\n\nDette tok meg ${tidsbruk.toString(SECONDS)} å finne ut av, så nå forventer jeg en innsats også fra deres side :meow_tired:"

            melding += "\n\n - Deres erbødig SPaghet :spaghet:"

            val slackmelding = JsonMessage.newMessage("slackmelding", mapOf(
                "melding" to melding,
                "level" to "ERROR",
                "system_participating_services" to packet["system_participating_services"]
            )).toJson()

            context.publish(slackmelding)
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
    }
}