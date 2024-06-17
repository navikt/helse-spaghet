package no.nav.helse.ventetilstand

import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import org.slf4j.LoggerFactory
import java.util.*

internal class VedtaksperiodeEndretRiver(
    rapidApplication: RapidsConnection,
    private vararg val dao: VedtaksperiodeVentetilstandDao
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
        dao.venterIkke(vedtaksperiodeId)
        val vedtaksperiodeVentet = dao.hentOmVenter(vedtaksperiodeId) ?: return
        dao.venterIkke(vedtaksperiodeVentet, packet.hendelse)
        logger.info("Venter ikke lenger for {} som har gått fra $forrigeTilstand til $gjeldendeTilstand", keyValue("vedtaksperiodeId", vedtaksperiodeId))
    }

    private companion object {
        val logger = LoggerFactory.getLogger(VedtaksperiodeEndretRiver::class.java)
    }
}