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
            val slackmelding = JsonMessage.newMessage(
                "slackmelding", mapOf(
                    "melding" to "$melding\n\n",
                    "level" to "INFO",
                    "channel" to "G01BRRU3666", // wip
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

        private fun lagMelding(oppsummering: List<VedtaksperiodeVentetilstandDao.VentegruppeExternal>): String {
            var melding = """
            God mandag! :monday: 
            
            Her er et øyeblikksbilde over ventetid for de sykmeldte før de får sykepengesøknaden sin ferdig behandlet :sonic-waiting:
            Denne er spennende å følge med på og kan hjelpe oss å identifisere eventuelle forsinkelser :excited:  
            Ønsket med denne informasjonen er å synliggjøre hvor mange perioder som venter på behandling og hvorfor :lets_go: 
            
            
            """.trimIndent()
            val ventegrupper = oppsummering.groupBy { it.årsak }
            ventegrupper.forEach { (gruppe, data) ->
                melding += "*Perioder som har ventet på ${gruppe.hvem()}* \n"
                data.forEach {
                    melding += "\t ${it.bucket.emoji()} ${it.bucket.lowercase()}: *${it.antall}* \n"
                }
                melding += "\n"
            }
            return melding
        }

        @JvmStatic
        fun main(args: Array<String>) {
            val enOppsummering = """
                291,INFORMASJON FRA ARBEIDSGIVER,UNDER 30 DAGER
                9815,INFORMASJON FRA ARBEIDSGIVER,MELLOM 30 OG 90 DAGER
                6652,INFORMASJON FRA ARBEIDSGIVER,OVER 90 DAGER
                164,SAKSBEHANDLER,UNDER 30 DAGER
                2048,SAKSBEHANDLER,MELLOM 30 OG 90 DAGER
                895,SAKSBEHANDLER,OVER 90 DAGER
                127,SØKNAD,MELLOM 30 OG 90 DAGER
            """.trimIndent().lines().map {
                val split = it.split(",")
                VedtaksperiodeVentetilstandDao.VentegruppeExternal(
                    årsak = split[1],
                    antall = split[0].toInt(),
                    bucket = split[2])
            }
            println(lagMelding(oppsummering = enOppsummering))
        }
    }
}