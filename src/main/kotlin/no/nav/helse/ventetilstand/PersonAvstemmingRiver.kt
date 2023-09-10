package no.nav.helse.ventetilstand

import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import org.slf4j.LoggerFactory
import java.util.*
import javax.sql.DataSource

internal class PersonAvstemmingRiver(
    rapidApplication: RapidsConnection,
    dataSource: DataSource
) : River.PacketListener {
    private val vedtaksperiodeVentetilstandDao = VedtaksperiodeVentetilstandDao(dataSource)

    init {
        River(rapidApplication).apply {
            validate { it.demandValue("@event_name", "person_avstemt") }
            validate {
                it.requireKey("@id", "aktørId", "fødselsnummer")
                it.requireArray("arbeidsgivere") {
                    requireKey("organisasjonsnummer")
                    requireArray("forkastedeVedtaksperioder") {
                        requireKey("id", "tilstand")
                    }
                }
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        packet["arbeidsgivere"].forEach { arbeidsgiver ->
            arbeidsgiver.path("forkastedeVedtaksperioder").forEach { forkastetVedtaksperiode ->
                val vedtaksperiodeId = UUID.fromString(forkastetVedtaksperiode.path("id").asText())
                vedtaksperiodeVentetilstandDao.hentOmVenter(vedtaksperiodeId)?.also { vedtaksperiodeVentet ->
                    vedtaksperiodeVentetilstandDao.venterIkke(vedtaksperiodeVentet, packet.hendelse)
                    logger.info("Venter ikke lenger for {} som har blitt forkastet", keyValue("vedtaksperiodeId", vedtaksperiodeId))
                }
            }
        }
    }

    private companion object {
        val logger = LoggerFactory.getLogger(PersonAvstemmingRiver::class.java)
    }
}