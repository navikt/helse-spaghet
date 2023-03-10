package no.nav.helse.ventetilstand

import kotliquery.Row
import no.nav.helse.Util.uuid
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.asLocalDateTime
import no.nav.helse.rapids_rivers.isMissingOrNull
import java.time.LocalDateTime
import java.util.*

data class VedtaksperiodeVenter (
    internal val vedtaksperiodeId: UUID,
    internal val fødselsnummer: String,
    internal val organisasjonsnummer: String,
    internal val ventetSiden: LocalDateTime,
    internal val venterTil: LocalDateTime,
    internal val venterPå: VenterPå,
)

data class VenterPå (
    internal val vedtaksperiodeId: UUID,
    internal val organisasjonsnummer: String,
    internal val hva: String,
    internal val hvorfor: String?,
)

internal val JsonMessage.vedtaksperiodeVenter get() = VedtaksperiodeVenter(
    vedtaksperiodeId = UUID.fromString(this["vedtaksperiodeId"].asText()),
    fødselsnummer = this["fødselsnummer"].asText(),
    organisasjonsnummer = this["organisasjonsnummer"].asText(),
    ventetSiden = this["ventetSiden"].asLocalDateTime(),
    venterTil = this["venterTil"].asLocalDateTime(),
    venterPå = VenterPå(
        vedtaksperiodeId = UUID.fromString(this["venterPå.vedtaksperiodeId"].asText()),
        organisasjonsnummer = this["venterPå.organisasjonsnummer"].asText(),
        hva = this["venterPå.venteårsak.hva"].asText(),
        hvorfor = this["venterPå.venteårsak.hvorfor"].takeUnless { it.isMissingOrNull() }?.asText()
    )
)

internal val Row.vedtaksperiodeVenter get() = VedtaksperiodeVenter(
    vedtaksperiodeId = this.uuid("vedtaksperiodeId"),
    fødselsnummer = this.string("fødselsnummer"),
    organisasjonsnummer = this.string("organisasjonsnummer"),
    ventetSiden = this.localDateTime("ventetSiden"),
    venterTil = this.localDateTime("venterTil"),
    venterPå = VenterPå(
        vedtaksperiodeId = this.uuid("venterPåVedtaksperiodeId"),
        organisasjonsnummer = this.string("venterPåOrganisasjonsnummer"),
        hva = this.string("venterPåHva"),
        hvorfor = this.stringOrNull("venterPåHvorfor")
    )
)

