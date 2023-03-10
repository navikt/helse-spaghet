package no.nav.helse.ventetilstand

import ch.qos.logback.classic.Logger
import com.fasterxml.jackson.databind.JsonNode
import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.helse.rapids_rivers.*
import org.slf4j.LoggerFactory
import java.util.*
import javax.sql.DataSource

internal class VedtaksperiodeVenterRiver (
    rapidApplication: RapidsConnection,
    private val vedtaksperiodeVentetilstandDao: VedtaksperiodeVentetilstandDao
    ) : River.PacketListener {

    private companion object {
        val logger = LoggerFactory.getLogger(VedtaksperiodeVenterRiver::class.java)
    }
    init {
        River(rapidApplication).apply {
            validate { it.requireValue("@event_name", "vedtaksperiode_venter") }
            validate { it.requireKey(
                "venterPå.venteårsak.hva",
                "venterPå.vedtaksperiodeId",
                "venterPå.organisasjonsnummer"
            ) }
            validate { it.interestedIn("venterPå.venteårsak.hvorfor") }
            validate { it.requireKey(
                "@id",
                "vedtaksperiodeId",
                "organisasjonsnummer",
                "ventetSiden",
                "venterTil",
                "fødselsnummer"
            ) }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val ny = packet.vedtaksperiodeVenter
        val vedtaksperiodeId = UUID.fromString(packet["vedtaksperiodeId"].asText())
        val gammel = vedtaksperiodeVentetilstandDao.hentOmVenter(vedtaksperiodeId)
        if (ny == gammel) return logger.info("Ingen endring på ventetilstand for {}", keyValue("vedtaksperiodeId", vedtaksperiodeId))
        vedtaksperiodeVentetilstandDao.venter(ny, packet.hendelse)
        logger.info("Lagret ny ventetilstand for {}", keyValue("vedtaksperiodeId", vedtaksperiodeId))
    }
}