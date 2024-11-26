package no.nav.helse.ventetilstand

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.module.kotlin.convertValue
import com.github.navikt.tbd_libs.rapids_and_rivers.*
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import no.nav.helse.objectMapper
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*

internal class VedtaksperiodeVenterRiver (
    rapidApplication: RapidsConnection,
    private val dao: VedtaksperiodeVentetilstandDao
) : River.PacketListener {

    init {
        River(rapidApplication).apply {
            precondition { it.requireValue("@event_name", "vedtaksperioder_venter") }
            validate { it.requireKey("@id", "fødselsnummer") }
            validate {
                it.requireArray("vedtaksperioder") {
                    requireKey(
                        "venterPå.venteårsak.hva",
                        "venterPå.vedtaksperiodeId",
                        "venterPå.skjæringstidspunkt",
                        "venterPå.organisasjonsnummer"
                    )
                    interestedIn("venterPå.venteårsak.hvorfor")
                    requireKey("vedtaksperiodeId", "skjæringstidspunkt", "organisasjonsnummer", "ventetSiden", "venterTil")
                }
            }
        }.register(this)

        // todo: deprecated river
        River(rapidApplication).apply {
            precondition { it.requireValue("@event_name", "vedtaksperiode_venter") }
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
        }.register(object : River.PacketListener {
            override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
                dao.venter(packet.vedtaksperiodeVenter, packet.hendelse)
            }
        })
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        packet["vedtaksperioder"].forEach { t ->
            val dto = objectMapper.convertValue<VedtaksperiodeVenterDto>(t)
            dao.venter(VedtaksperiodeVenter.opprett(
                vedtaksperiodeId = dto.vedtaksperiodeId,
                skjæringstidspunkt = dto.skjæringstidspunkt,
                fødselsnummer = packet["fødselsnummer"].asText(),
                organisasjonsnummer = dto.organisasjonsnummer,
                ventetSiden = dto.ventetSiden,
                venterTil = dto.venterTil,
                venterPå = VenterPå(
                    vedtaksperiodeId = dto.venterPå.vedtaksperiodeId,
                    organisasjonsnummer = dto.venterPå.organisasjonsnummer,
                    skjæringstidspunkt = dto.venterPå.skjæringstidspunkt,
                    hva = dto.venterPå.venteårsak.hva,
                    hvorfor = dto.venterPå.venteårsak.hvorfor
                )
            ), packet.hendelse)
        }
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

@JsonIgnoreProperties(ignoreUnknown = true)
private data class VedtaksperiodeVenterDto(
    val organisasjonsnummer: String,
    val vedtaksperiodeId: UUID,
    val skjæringstidspunkt: LocalDate,
    val ventetSiden: LocalDateTime,
    val venterTil: LocalDateTime,
    val venterPå: VenterPå
) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    data class VenterPå(
        val vedtaksperiodeId: UUID,
        val organisasjonsnummer: String,
        val skjæringstidspunkt: LocalDate,
        val venteårsak: Venteårsak
    )

    @JsonIgnoreProperties(ignoreUnknown = true)
    data class Venteårsak(
        val hva : String,
        val hvorfor: String?
    )
}