package no.nav.helse.ventetilstand

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import net.logstash.logback.argument.StructuredArguments.keyValue
import org.slf4j.LoggerFactory
import java.util.*

internal class VedtaksperiodeEndretRiver(
    rapidApplication: RapidsConnection,
    private val dao: VedtaksperiodeVentetilstandDao
) : River.PacketListener {


    init {
        River(rapidApplication).apply {
            validate { it.demandValue("@event_name", "vedtaksperiode_endret") }
            validate { it.requireKey(
                "@id",
                "vedtaksperiodeId",
                "organisasjonsnummer",
                "fødselsnummer",
                "gjeldendeTilstand",
                "forrigeTilstand"
            ) }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val gjeldendeTilstand = packet["gjeldendeTilstand"].asText()
        val forrigeTilstand = packet["forrigeTilstand"].asText()
        if (gjeldendeTilstand == forrigeTilstand) return
        val vedtaksperiodeId = UUID.fromString(packet["vedtaksperiodeId"].asText())
        dao.venterIkke(vedtaksperiodeId, packet.hendelse)
        logger.info("Venter ikke lenger for {} som har gått fra $forrigeTilstand til $gjeldendeTilstand", keyValue("vedtaksperiodeId", vedtaksperiodeId))
    }

    private companion object {
        val logger = LoggerFactory.getLogger(VedtaksperiodeEndretRiver::class.java)
    }
}