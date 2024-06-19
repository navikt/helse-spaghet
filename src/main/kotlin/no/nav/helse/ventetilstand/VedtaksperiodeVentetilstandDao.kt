package no.nav.helse.ventetilstand

import java.util.*

internal interface VedtaksperiodeVentetilstandDao {
    fun venter(vedtaksperiodeVenter: VedtaksperiodeVenter, hendelse: Hendelse)
    fun venterIkke(vedtaksperiodeId: UUID, hendelse: Hendelse)
}
