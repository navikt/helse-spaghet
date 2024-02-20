package no.nav.helse.ventetilstand

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit.SECONDS
import java.util.*

internal data class VedtaksperiodeVenter private constructor(
    internal val vedtaksperiodeId: UUID,
    internal val skjæringstidspunkt: LocalDate,
    internal val fødselsnummer: String,
    internal val organisasjonsnummer: String,
    internal val ventetSiden: LocalDateTime,
    internal val venterTil: LocalDateTime,
    internal val venterPå: VenterPå
) {
    internal companion object {
        private val MAX = LocalDateTime.MAX.withYear(9999)
        private val LocalDateTime.sanitize get() = coerceAtMost(MAX).truncatedTo(SECONDS)
        internal fun opprett(vedtaksperiodeId: UUID, skjæringstidspunkt: LocalDate, fødselsnummer: String, organisasjonsnummer: String, ventetSiden: LocalDateTime, venterTil: LocalDateTime, venterPå: VenterPå) =
            VedtaksperiodeVenter(vedtaksperiodeId, skjæringstidspunkt, fødselsnummer, organisasjonsnummer, ventetSiden.sanitize, venterTil.sanitize, venterPå)
    }
}

internal data class VenterPå(
    internal val vedtaksperiodeId: UUID,
    internal val skjæringstidspunkt: LocalDate,
    internal val organisasjonsnummer: String,
    internal val hva: String,
    internal val hvorfor: String?
)

