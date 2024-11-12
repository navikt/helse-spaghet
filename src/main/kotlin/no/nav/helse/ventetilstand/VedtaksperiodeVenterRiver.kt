package no.nav.helse.ventetilstand

import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDate
import com.github.navikt.tbd_libs.rapids_and_rivers.isMissingOrNull
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import io.micrometer.core.instrument.MeterRegistry
import java.util.*

internal class VedtaksperiodeVenterRiver (
    rapidApplication: RapidsConnection,
    private val dao: VedtaksperiodeVentetilstandDao
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

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        dao.venter(packet.vedtaksperiodeVenter, packet.hendelse)
    }

    private companion object {
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