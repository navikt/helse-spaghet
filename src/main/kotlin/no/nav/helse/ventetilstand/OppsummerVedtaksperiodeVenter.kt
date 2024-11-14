package no.nav.helse.ventetilstand

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import no.nav.helse.ventetilstand.OppsummeringDao.Ventegruppe
import no.nav.helse.ventetilstand.Slack.sendPåSlack
import org.slf4j.LoggerFactory
import org.slf4j.event.Level.INFO
import kotlin.random.Random.Default.nextInt

internal class OppsummerVedtaksperiodeVenter (
    rapidsConnection: RapidsConnection,
    private val dao: OppsummeringDao
): River.PacketListener {

    init {
        River(rapidsConnection).apply {
            precondition {
                it.requireValue("@event_name", "oppsummer_vedtaksperiode_venter")
                it.forbid("simplified_for_external_purposes")
            }
            validate {
                it.requireKey("system_participating_services")
            }
        }.register(this)
        River(rapidsConnection).apply {
            precondition {
                it.requireValue("@event_name", "halv_time")
                it.requireValue("time", 9)
                it.requireValue("minutt", 30)
                it.requireAny("ukedag", listOf("MONDAY", "FRIDAY"))
            }
            validate {
                it.requireKey("system_participating_services")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        try {
            val (oppsummering, antallPersoner) = dao.oppsummering()
            val melding = lagMelding(oppsummering, antallPersoner)
            context.sendPåSlack(packet, INFO, melding)
        } catch (exception: Exception) {
            sikkerlogg.error("Feil ved generering av oppsummering for vedtaksperioder som venter", exception)
        }
    }

    private companion object {
        private val sikkerlogg = LoggerFactory.getLogger("tjenestekall")
        private val Int.fintAntall get() = "$this".padStart(10,' ')
        private val String.skummel get() = setOf("hjelp", "utbetaling", "beregning").any { this.startsWith(it) }
        private val String.stuck get() = this.endsWith("måneder")
        private val String.finÅrsak get() = replace("_", " ").lowercase().let {
            if (it.skummel) "$it $etUndrendeSmilefjes"
            else if (it.stuck) "<https://www.nav.no/saksbehandlingstider#sykepenger|$it> :look-away:"
            else it
        }

        private val undrendeSmilefjes = setOf("hmm-skull", "hmmnani", "hmmmmm", "thinking-taco", "pepewtf", "gull_scream", "elmo_what", "pepewhat", "wait-what", "this-is-fine-fire", "huh_what_husky", "pepe-hmm").map { ":$it:" }
        private val etUndrendeSmilefjes get() =  nextInt(undrendeSmilefjes.size - 1).let { undrendeSmilefjes[it] }

        private fun Int.finProsentAv(total: Int) = ((this.toDouble() / total) * 100).let {
            val prosent = String.format("%.2f", it)
            if ("0.00" == prosent || "0,00" == prosent) ""
            else "($prosent%)"
        }

        private fun lagMelding(oppsummering: List<Ventegruppe>, antallPersoner: Int): String {
            val antallVedtaksperioder = oppsummering.sumOf { it.antall }
            val propper = oppsummering.filter { it.propp }
            val ettergølgende = oppsummering.filterNot { it.propp }.sumOf { it.antall }

            var melding =
                "\n\nDet er $antallVedtaksperioder vedtaksperioder fordelt på $antallPersoner sykmeldte som venter i systemet ⏳\n\n"

            val antallPropper = propper.sumOf { it.antall }
            melding += "Av disse er det $antallPropper som venter på noen direkte:\n"
            propper.groupBy { it.venterPå }.mapValues { (_, gruppe) -> gruppe.sumOf { it.antall } }.entries.sortedByDescending { it.value }.forEach { (venterPå, antall) ->
                melding += "${antall.fintAntall} venter på $venterPå ${antall.finProsentAv(antallPropper)}\n"
            }

            melding += "\nNærmere bestemt venter de på:\n"
            propper.forEach { (årsak, antall) ->
                melding += "${antall.fintAntall} venter på ${årsak.finÅrsak} ${antall.finProsentAv(antallPropper)}\n"
            }

            melding += "\nDe resterende $ettergølgende står bak en av ☝️ og venter tålmodig :sonic-waiting:\n"

            return melding
        }

        private val Ventegruppe.venterPå get() = when {
            årsak.startsWith("INNTEKTSMELDING") -> "arbeidsgiver :briefcase:"
            årsak.startsWith("GODKJENNING") -> "saksbehandler :female-office-worker:"
            årsak.startsWith("SØKNAD") -> "sykmeldte :pepesick:"
            else -> "tverrfaglig manuell hjelp :maxi-nut-cracker: :david-gun: :eminott_aminott_nottland_onduty:"
        }
    }
}