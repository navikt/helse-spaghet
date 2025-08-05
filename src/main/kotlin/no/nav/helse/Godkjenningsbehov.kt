package no.nav.helse

import java.time.LocalDateTime
import java.util.*

class Godkjenningsbehov(
    val vedtaksperiodeId: UUID,
    val aktørId: String,
    val fødselsnummer: String,
    val periodetype: String,
    val inntektskilde: String,
    val løsning: Løsning,
    val utbetalingType: String,
    val refusjonType: String?,
    val saksbehandleroverstyringer: List<UUID>,
    val behandlingId: UUID,
) {
    data class Løsning(
        val godkjent: Boolean,
        val saksbehandlerIdent: String,
        val godkjentTidspunkt: LocalDateTime,
        val årsak: String?,
        val begrunnelser: List<String>?,
        val kommentar: String?,
    )
}
