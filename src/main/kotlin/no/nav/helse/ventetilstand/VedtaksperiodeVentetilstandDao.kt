package no.nav.helse.ventetilstand

import kotliquery.queryOf
import kotliquery.sessionOf
import org.intellij.lang.annotations.Language
import java.util.UUID
import javax.sql.DataSource

internal class VedtaksperiodeVentetilstandDao(private val dataSource: DataSource) {

    fun hentOmVenter(vedtaksperiodeId: UUID): VedtaksperiodeVenter? {
        return sessionOf(dataSource).use { session ->
             session.single(
                 queryOf(HENT_OM_VENTER, mapOf("vedtaksperiodeId" to vedtaksperiodeId))
             ){ row ->
                if (!row.boolean("venter")) null
                else row.vedtaksperiodeVenter
             }
        }
    }

    fun venter(vedtaksperiodeVenter: VedtaksperiodeVenter, hendelse: Hendelse) {
        return sessionOf(dataSource).use {session ->
            session.update(
                queryOf(VENTER, mapOf(
                    "hendelseId" to hendelse.id,
                    "hendelse" to hendelse.hendelse,
                    "vedtaksperiodeId" to vedtaksperiodeVenter.vedtaksperiodeId,
                    "organisasjonsnummer" to vedtaksperiodeVenter.organisasjonsnummer,
                    "ventetSiden" to vedtaksperiodeVenter.ventetSiden,
                    "venterTil" to vedtaksperiodeVenter.venterTil,
                    "venterPåVedtaksperiodeId" to vedtaksperiodeVenter.venterPå.vedtaksperiodeId,
                    "venterPåOrganisasjonsnummer" to vedtaksperiodeVenter.venterPå.organisasjonsnummer,
                    "venterPåHva" to vedtaksperiodeVenter.venterPå.hva,
                    "venterPåHvorfor" to vedtaksperiodeVenter.venterPå.hvorfor)
                )
            )
        }
    }

    fun venterIkke(vedtaksperiodeVentet: VedtaksperiodeVenter, hendelse: Hendelse) {
        return sessionOf(dataSource).use {session ->
            session.update(
                queryOf(
                    VENTER_IKKE, mapOf(
                    "hendelseId" to hendelse.id,
                    "hendelse" to hendelse.hendelse,
                    "vedtaksperiodeId" to vedtaksperiodeVentet.vedtaksperiodeId,
                    "organisasjonsnummer" to vedtaksperiodeVentet.organisasjonsnummer)
                )
            )
        }
    }

    private companion object {
        @Language("PostgreSQL")
        val HENT_OM_VENTER = "SELECT * FROM vedtaksperiode_ventetilstand WHERE vedtaksperiodeId = :vedtaksperiodeId ORDER BY tidsstempel DESC LIMIT 1"

        @Language("PostgreSQL")
        val VENTER = """
            INSERT INTO vedtaksperiode_ventetilstand(hendelseId, hendelse, venter, vedtaksperiodeId, organisasjonsnummer, ventetSiden, venterTil, venterPåVedtaksperiodeId, venterPåOrganisasjonsnummer, venterPåHva, venterPåHvorfor)
            VALUES (:hendelseId, :hendelse, true, :vedtaksperiodeId, :organisasjonsnummer, :ventetSiden, :venterTil, :venterPåVedtaksperiodeId, :venterPåOrganisasjonsnummer, :venterPåHva, :venterPåHvorfor) 
        """

        @Language("PostgreSQL")
        val VENTER_IKKE = """
            INSERT INTO vedtaksperiode_ventetilstand(hendelseId, hendelse, venter, vedtaksperiodeId, organisasjonsnummer, ventetSiden, venterTil, venterPåVedtaksperiodeId, venterPåOrganisasjonsnummer, venterPåHva, venterPåHvorfor)
            VALUES (:hendelseId, :hendelse, false, :vedtaksperiodeId, :organisasjonsnummer, null, null, null, null, null, null)  
        """
    }
}