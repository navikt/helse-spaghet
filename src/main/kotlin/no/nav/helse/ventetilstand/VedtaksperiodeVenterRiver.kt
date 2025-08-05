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

internal class VedtaksperiodeVenterRiver(
    rapidApplication: RapidsConnection,
    private val dao: VedtaksperiodeVentetilstandDao,
) : River.PacketListener {
    init {
        River(rapidApplication)
            .apply {
                precondition { it.requireValue("@event_name", "vedtaksperioder_venter") }
                validate { it.requireKey("@id", "fødselsnummer") }
                validate {
                    it.requireArray("vedtaksperioder") {
                        requireKey(
                            "venterPå.venteårsak.hva",
                            "venterPå.vedtaksperiodeId",
                            "venterPå.skjæringstidspunkt",
                            "venterPå.organisasjonsnummer",
                        )
                        interestedIn("venterPå.venteårsak.hvorfor")
                        requireKey("vedtaksperiodeId", "skjæringstidspunkt", "organisasjonsnummer", "ventetSiden", "venterTil")
                    }
                }
            }.register(this)
    }

    override fun onPacket(
        packet: JsonMessage,
        context: MessageContext,
        metadata: MessageMetadata,
        meterRegistry: MeterRegistry,
    ) {
        val fødselsnummer = packet["fødselsnummer"].asText()
        val vedtaksperiodeVenter =
            packet["vedtaksperioder"]
                .map { json -> objectMapper.convertValue<VedtaksperiodeVenterDto>(json) }
                .map { dto ->
                    VedtaksperiodeVenter.opprett(
                        vedtaksperiodeId = dto.vedtaksperiodeId,
                        skjæringstidspunkt = dto.skjæringstidspunkt,
                        fødselsnummer = fødselsnummer,
                        organisasjonsnummer = dto.organisasjonsnummer,
                        ventetSiden = dto.ventetSiden,
                        venterTil = dto.venterTil,
                        venterPå =
                            VenterPå(
                                vedtaksperiodeId = dto.venterPå.vedtaksperiodeId,
                                organisasjonsnummer = dto.venterPå.organisasjonsnummer,
                                skjæringstidspunkt = dto.venterPå.skjæringstidspunkt,
                                hva = dto.venterPå.venteårsak.hva,
                                hvorfor = dto.venterPå.venteårsak.hvorfor,
                            ),
                    )
                }
        dao.venter(fødselsnummer, vedtaksperiodeVenter, packet.hendelse)
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
private data class VedtaksperiodeVenterDto(
    val organisasjonsnummer: String,
    val vedtaksperiodeId: UUID,
    val skjæringstidspunkt: LocalDate,
    val ventetSiden: LocalDateTime,
    val venterTil: LocalDateTime,
    val venterPå: VenterPå,
) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    data class VenterPå(
        val vedtaksperiodeId: UUID,
        val organisasjonsnummer: String,
        val skjæringstidspunkt: LocalDate,
        val venteårsak: Venteårsak,
    )

    @JsonIgnoreProperties(ignoreUnknown = true)
    data class Venteårsak(
        val hva: String,
        val hvorfor: String?,
    )
}
