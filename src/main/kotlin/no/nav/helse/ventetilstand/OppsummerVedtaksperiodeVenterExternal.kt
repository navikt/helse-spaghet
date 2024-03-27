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
        private val Int.fintAntall get() = "$this".padStart(10,' ')

        private fun lagMelding(oppsummering: List<VedtaksperiodeVentetilstandDao.VentegruppeExternal>): String {
            var melding = """\n\n God mandag! :monday: 
                Her er et øyeblikksbilde over ventetid for de sykmeldte før de får sykepengesøknaden sin ferdig behandlet :sonic-waiting:
                Denne er spennende å følge med på og kan hjelpe oss å identifisere eventuelle forsinkelser :excited:  
                Målet med denne informasjonen er å dele data fra vedtaksløsningen med resten av området :lets_go:
                Målet med denne informasjonen er å sikre felles oversikt for området og å fremme kontinuerlig forbedring av tjenestene :lets_go: \n\n""".trimMargin()
            melding += "\nNærmere bestemt venter de på:\n"
            oppsummering.forEach { (årsak, antall, ventet_i) ->
                melding += "${antall.fintAntall} venter på $årsak og har ventet i ${ventet_i}\n"
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