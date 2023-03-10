package no.nav.helse.ventetilstand

import java.time.LocalDateTime
import java.util.*

data class VedtaksperiodeVenter (
    internal val vedtaksperiodeId: UUID,
    internal val fødselsnummer: String,
    internal val organisasjonsnummer: String,
    internal val ventetSiden: LocalDateTime,
    internal val venterTil: LocalDateTime,
    internal val venterPå: VenterPå
)

data class VenterPå (
    internal val vedtaksperiodeId: UUID,
    internal val organisasjonsnummer: String,
    internal val hva: String,
    internal val hvorfor: String?
)

