package no.nav.helse.ventetilstand

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import no.nav.helse.ventetilstand.OppsummeringDao.VentegruppeExternal
import org.slf4j.LoggerFactory

internal class OppsummerVedtaksperiodeVenterExternal (
    rapidsConnection: RapidsConnection,
    private val dao: OppsummeringDao
): River.PacketListener {

    init {
        River(rapidsConnection).apply {
            validate {
                it.demandValue("@event_name", "oppsummer_vedtaksperiode_venter")
                it.requireKey("system_participating_services", "simplified_for_external_purposes")
            }
        }.register(this)
        River(rapidsConnection).apply {
            validate {
                it.demandValue("@event_name", "halv_time")
                it.demandValue("time", 8)
                it.demandValue("minutt", 30)
                it.demandAny("ukedag", listOf("MONDAY"))
                it.requireKey("system_participating_services")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        try {
            val oppsummering = dao.oppsummeringExternal()
            val melding = lagMelding(oppsummering)
            val slackmelding = JsonMessage.newMessage(
                "slackmelding", mapOf(
                    "melding" to "$melding\n\n",
                    "level" to "INFO",
                    "channel" to "GE89UHQM7",
                    "utenPrefix" to true,
                    "utenSuffix" to true,
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

        private fun String.emoji() = when(this) {
            "UNDER 30 DAGER" -> ":large_green_circle:"
            "MELLOM 30 OG 90 DAGER" -> ":large_yellow_circle:"
            "OVER 90 DAGER" -> ":red_circle:"
            else -> ""
        }

        private fun String.hvem() = when(this) {
            "INFORMASJON FRA ARBEIDSGIVER" -> "arbeidsgiver"
            "SAKSBEHANDLER" -> "saksbehandler"
            "SØKNAD" -> "den sykmeldte"
            else -> this
        }

        private fun lagMelding(oppsummering: List<VentegruppeExternal>): String {
            var melding = """
            God mandag! :monday: 
            
            Her er et øyeblikksbilde over ventetid for de sykmeldte før de får sykepengesøknaden sin ferdig behandlet :sonic-waiting:
            
            Ønsket med denne informasjonen er å synliggjøre hvor mange perioder som venter på behandling og hvorfor :excited: :lets_go:
            
            
            """.trimIndent()
            val ventegrupper = oppsummering.groupBy { it.årsak }
            ventegrupper.forEach { (gruppe, data) ->
                melding += "*Har ventet på ${gruppe.hvem()}* \n"
                data.forEach {
                    melding += "\t ${it.bucket.emoji()} ${it.bucket.lowercase()}: *${it.antall}* \n"
                }
                melding += "\n"
            }

            melding += """
            *Ettertanker*
                1. Det er bare saker som venter i ny løsning som er omfattet
                2. Det er ikke den totale ventetiden som vises, kun ventetiden i nåværende tilstand. Man kan vente over 90 dager på arbeidsgiver, for så å vente over 90 dager på saksbehandler.

            Med vennlig hilsen,
            Viggo Velferdsvenn
            """.trimIndent()
            return melding
        }
    }
}