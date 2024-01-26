package no.nav.helse.ventetilstand

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import no.nav.helse.ventetilstand.Slack.sendPåSlack
import no.nav.helse.ventetilstand.VedtaksperiodeVentetilstandDao.Ventegruppe
import org.slf4j.LoggerFactory
import org.slf4j.event.Level.INFO
import javax.sql.DataSource
import kotlin.random.Random.Default.nextInt

internal class OppsummerVedtaksperiodeVenter (
    rapidsConnection: RapidsConnection,
    dataSource: DataSource
): River.PacketListener {
    private val dao = VedtaksperiodeVentetilstandDao(dataSource)

    init {
        River(rapidsConnection).apply {
            validate {
                it.demandValue("@event_name", "oppsummer_vedtaksperiode_venter")
                it.requireKey("system_participating_services")
            }
        }.register(this)
        River(rapidsConnection).apply {
            validate {
                it.demandValue("@event_name", "halv_time")
                it.demandValue("time", 9)
                it.demandValue("minutt", 30)
                it.demandValue("ukedag", "FRIDAY")
                it.requireKey("system_participating_services")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
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

        @JvmStatic
        fun main(args: Array<String>) {
            val enOppsummering = """
                INNTEKTSMELDING,true,33376
                GODKJENNING,false,14300
                GODKJENNING_FORDI_OVERSTYRING_IGANGSATT,false,11327
                GODKJENNING,true,10575
                GODKJENNING_FORDI_OVERSTYRING_IGANGSATT,true,4194
                INNTEKTSMELDING,false,3424
                INNTEKTSMELDING_FORDI_MANGLER_INNTEKT_FOR_VILKÅRSPRØVING_PÅ_ANDRE_ARBEIDSGIVERE,true,338
                INNTEKTSMELDING_FORDI_MANGLER_INNTEKT_FOR_VILKÅRSPRØVING_PÅ_ANDRE_ARBEIDSGIVERE,false,319
                SØKNAD_FORDI_HAR_SYKMELDING_SOM_OVERLAPPER_PÅ_ANDRE_ARBEIDSGIVERE,true,171
                INNTEKTSMELDING_FORDI_MANGLER_REFUSJONSOPPLYSNINGER_PÅ_ANDRE_ARBEIDSGIVERE,false,94
                SØKNAD_FORDI_HAR_SYKMELDING_SOM_OVERLAPPER_PÅ_ANDRE_ARBEIDSGIVERE,false,87
                INNTEKTSMELDING_FORDI_MANGLER_REFUSJONSOPPLYSNINGER_PÅ_ANDRE_ARBEIDSGIVERE,true,59
                INNTEKTSMELDING_FORDI_MANGLER_TILSTREKKELIG_INFORMASJON_TIL_UTBETALING_SAMME_ARBEIDSGIVE,false,37
                INNTEKTSMELDING_FORDI_MANGLER_TILSTREKKELIG_INFORMASJON_TIL_UTBETALING_SAMME_ARBEIDSGIVE,true,15
                HJELP_FORDI_VIL_UTBETALES,true,2
                BEREGNING_FORDI_OVERSTYRING_IGANGSATT,true,1
                BEREGNING_FORDI_OVERSTYRING_IGANGSATT,false,1
            """.trimIndent().lines().map {
                val split = it.split(",")
                Ventegruppe(split[0], split[2].toInt(), split[1].toBoolean())
            }
            println(lagMelding(oppsummering = enOppsummering, antallPersoner = 33521))
        }
    }
}