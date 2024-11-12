package no.nav.helse.ventetilstand

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import net.logstash.logback.argument.StructuredArguments.keyValue
import org.slf4j.LoggerFactory
import java.util.*

internal class VedtaksperiodeVenterIkkeRiver(
    rapidApplication: RapidsConnection,
    private val dao: VedtaksperiodeVentetilstandDao
) : River.PacketListener {

    init {
        River(rapidApplication).apply {
            validate { it.demandValue("@event_name", "vedtaksperiode_venter_ikke") }
            validate { it.requireKey("@id", "vedtaksperiodeId") }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        val vedtaksperiodeId = UUID.fromString(packet["vedtaksperiodeId"].asText())
        dao.venterIkke(vedtaksperiodeId, packet.hendelse)
        logger.info("Venter ikke lenger for {}. Har fått eksplisitt signal om at den ikke venter", keyValue("vedtaksperiodeId", vedtaksperiodeId))
    }

    private companion object {
        val logger = LoggerFactory.getLogger(VedtaksperiodeVenterIkkeRiver::class.java)
    }
}