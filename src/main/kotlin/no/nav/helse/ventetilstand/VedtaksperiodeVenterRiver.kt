package no.nav.helse.ventetilstand

import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.helse.rapids_rivers.*
import org.slf4j.LoggerFactory
import java.util.*

internal class VedtaksperiodeVenterRiver (
    rapidApplication: RapidsConnection,
    private vararg val dao: VedtaksperiodeVentetilstandDao
) : River.PacketListener {

    init {
        River(rapidApplication).apply {
            validate { it.demandValue("@event_name", "vedtaksperiode_venter") }
            validate { it.requireKey(
                "venterPå.venteårsak.hva",
                "venterPå.vedtaksperiodeId",
                "venterPå.skjæringstidspunkt",
                "venterPå.organisasjonsnummer"
            ) }
            validate { it.interestedIn("venterPå.venteårsak.hvorfor") }
            validate { it.requireKey(
                "@id",
                "vedtaksperiodeId",
                "skjæringstidspunkt",
                "organisasjonsnummer",
                "ventetSiden",
                "venterTil",
                "fødselsnummer"
            ) }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val ny = packet.vedtaksperiodeVenter
        dao.forEach { it.håndterVedtaksperiodeVenter(ny, packet.hendelse) }
    }

    private fun VedtaksperiodeVentetilstandDao.håndterVedtaksperiodeVenter(ny: VedtaksperiodeVenter, hendelse: Hendelse) {
        val vedtaksperiodeId = ny.vedtaksperiodeId
        val gammel = hentOmVenter(vedtaksperiodeId)
        if (ny == gammel) return logger.info("Ingen endring på ventetilstand for {} i ${this::class.simpleName}", keyValue("vedtaksperiodeId", vedtaksperiodeId))
        venter(ny, hendelse)
        logger.info("Lagret ny ventetilstand for {} i ${this::class.simpleName}", keyValue("vedtaksperiodeId", vedtaksperiodeId))
    }

    private companion object {
        val logger = LoggerFactory.getLogger(VedtaksperiodeVenterRiver::class.java)
        val JsonMessage.vedtaksperiodeVenter get() = VedtaksperiodeVenter.opprett(
            vedtaksperiodeId = UUID.fromString(this["vedtaksperiodeId"].asText()),
            skjæringstidspunkt = this["skjæringstidspunkt"].asLocalDate(),
            fødselsnummer = this["fødselsnummer"].asText(),
            organisasjonsnummer = this["organisasjonsnummer"].asText(),
            ventetSiden = this["ventetSiden"].asLocalDateTime(),
            venterTil = this["venterTil"].asLocalDateTime(),
            venterPå = VenterPå(
                vedtaksperiodeId = UUID.fromString(this["venterPå.vedtaksperiodeId"].asText()),
                skjæringstidspunkt = this["venterPå.skjæringstidspunkt"].asLocalDate(),
                organisasjonsnummer = this["venterPå.organisasjonsnummer"].asText(),
                hva = this["venterPå.venteårsak.hva"].asText(),
                hvorfor = this["venterPå.venteårsak.hvorfor"].takeUnless { it.isMissingOrNull() }?.asText()
            )
        )
    }
}