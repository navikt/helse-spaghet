package no.nav.helse.ventetilstand

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import org.slf4j.LoggerFactory
import javax.sql.DataSource

internal class OppsummerVedtaksperiodeVenterExternal (
    rapidsConnection: RapidsConnection,
    dataSource: DataSource
): River.PacketListener {
    private val dao = VedtaksperiodeVentetilstandDao(dataSource)

    init {
        River(rapidsConnection).apply {
            validate {
                it.demandValue("@event_name", "oppsummer_vedtaksperiode_venter")
                it.requireKey("system_participating_services", "simplified_for_external_purposes")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        try {
            val oppsummering = dao.oppsummeringExternal()
            val melding = lagMelding(oppsummering)
            val slackmelding = JsonMessage.newMessage("slackmelding", mapOf(
                "melding" to "$melding\n\n",
                "level" to "INFO",
                "channel" to "G01BRRU3666", // wip
                "system_participating_services" to packet["system_participating_services"]
            )
            ).toJson()
            context.publish(slackmelding)
        } catch (exception: Exception) {
            sikkerlogg.error("Feil ved generering av oppsummering for vedtaksperioder som venter", exception)
        }
    }

    private companion object {
        private val sikkerlogg = LoggerFactory.getLogger("tjenestekall")
        private val Int.fintAntall get() = "$this".padStart(10,' ')

        private fun lagMelding(oppsummering: List<VedtaksperiodeVentetilstandDao.VentegruppeExternal>): String {
            var melding = "\n\n God mandag! :monday: Her er et øyeblikksbilde over ventetid for de sykmeldte før de får sykepengesøknaden sin behandlet :sonic-waiting:  Dette for å gi innsikt i effektiviteten av prosesseringstiden og for å identifisere eventuelle forsinkelser. Denne er spennende å følge med på over tid :excited:  Ventetid er beregnet fra datoen søknaden ble sendt inn og har med alle søknader der saken ikke er ferdigbehandlet. En person kan ha flere søknader i denne oversikten. Målet med denne informasjonen er å sikre felles oversikt for området og å fremme kontinuerlig forbedring av tjenestene :lets_go: \n\n"
            melding += "\nNærmere bestemt venter de på:\n"
            oppsummering.forEach { (årsak, antall, ventet_i) ->
                melding += "${antall.fintAntall} venter på $årsak og har ventet i ${ventet_i}\n"
            }
            return melding
        }
    }
}