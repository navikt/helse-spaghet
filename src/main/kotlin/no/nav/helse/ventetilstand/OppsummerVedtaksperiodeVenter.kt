package no.nav.helse.ventetilstand

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import no.nav.helse.ventetilstand.Slack.sendPåSlack
import org.slf4j.LoggerFactory
import org.slf4j.event.Level.INFO
import javax.sql.DataSource

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

            val antallVedtaksperioder = oppsummering.sumOf { it.antall }
            val propper = oppsummering.filter { it.propp }
            val ettergølgende = oppsummering.filterNot { it.propp }.sumOf { it.antall }

            var melding =
                "\n\nDet er $antallVedtaksperioder vedtaksperioder fordelt på $antallPersoner sykmeldte som venter i systemet ⏳\n\n"

            melding += "Av disse er det ${propper.sumOf { it.antall }} som venter på noe direkte:\n"
            propper.forEach { (årsak, antall) ->
                melding += "${antall.fintAntall} venter på ${årsak.finÅrsak}\n"
            }

            melding += "\n\nDe resterende $ettergølgende står bak en av ☝️ og venter tålmodig :sonic-waiting:\n"

            context.sendPåSlack(packet, INFO, melding)
        } catch (exception: Exception) {
            sikkerlogg.error("Feil ved generering av oppsummering for vedtaksperioder som venter", exception)
        }
    }

    private companion object {
        private val sikkerlogg = LoggerFactory.getLogger("tjenestekall")
        private val Int.fintAntall get() = "$this".padStart(10,' ')
        private val String.skummel get() = setOf("hjelp", "utbetaling", "beregning").any { this.startsWith(it) }
        private val String.finÅrsak get() = replace("_", " ").lowercase().let {
            if (it.skummel) "$it :pepe-hmm:"
            else it
        }
    }
}