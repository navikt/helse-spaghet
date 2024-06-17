package no.nav.helse.ventetilstand

import java.util.*

internal interface VedtaksperiodeVentetilstandDao {
    fun venter(vedtaksperiodeVenter: VedtaksperiodeVenter, hendelse: Hendelse)
    fun venterIkke(vedtaksperiodeId: UUID, hendelse: Hendelse)
}

internal fun Array<out VedtaksperiodeVentetilstandDao>.venter(vedtaksperiodeVenter: VedtaksperiodeVenter, hendelse: Hendelse) =
    forEach { it.venter(vedtaksperiodeVenter, hendelse) }

internal fun Array<out VedtaksperiodeVentetilstandDao>.venterIkke(vedtaksperiodeId: UUID, hendelse: Hendelse) =
    forEach { it.venterIkke(vedtaksperiodeId, hendelse) }

