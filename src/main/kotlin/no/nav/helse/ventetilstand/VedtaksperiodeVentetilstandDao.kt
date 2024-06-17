package no.nav.helse.ventetilstand

import java.util.*

internal interface VedtaksperiodeVentetilstandDao {
    fun hentOmVenter(vedtaksperiodeId: UUID): VedtaksperiodeVenter?
    fun venter(vedtaksperiodeVenter: VedtaksperiodeVenter, hendelse: Hendelse)
    fun venterIkke(vedtaksperiodeVentet: VedtaksperiodeVenter, hendelse: Hendelse)
    fun stuck(): List<VedtaksperiodeVenter>
}

internal fun Array<out VedtaksperiodeVentetilstandDao>.hentOmVenter(vedtaksperiodeId: UUID) =
    firstNotNullOfOrNull { it.hentOmVenter(vedtaksperiodeId) }

internal fun Array<out VedtaksperiodeVentetilstandDao>.venter(vedtaksperiodeVenter: VedtaksperiodeVenter, hendelse: Hendelse) =
    forEach { it.venter(vedtaksperiodeVenter, hendelse) }

internal fun Array<out VedtaksperiodeVentetilstandDao>.venterIkke(vedtaksperiodeVenter: VedtaksperiodeVenter, hendelse: Hendelse) =
    forEach { it.venterIkke(vedtaksperiodeVenter, hendelse) }

