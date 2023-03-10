package no.nav.helse.ventetilstand

import kotliquery.Row
import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.Util.uuid
import org.intellij.lang.annotations.Language
import java.time.LocalDateTime
import java.util.*
import javax.sql.DataSource

internal class VedtaksperiodeVentetilstandDao(private val dataSource: DataSource) {

    internal fun hentOmVenter(vedtaksperiodeId: UUID): VedtaksperiodeVenter? {
        return sessionOf(dataSource).use { session ->
             session.single(
                 queryOf(HENT_OM_VENTER, mapOf("vedtaksperiodeId" to vedtaksperiodeId))
             ){ row ->
                if (!row.boolean("venter")) null
                else row.vedtaksperiodeVenter
             }
        }
    }

    internal fun venter(vedtaksperiodeVenter: VedtaksperiodeVenter, hendelse: Hendelse) {
        return sessionOf(dataSource).use {session ->
            session.update(
                queryOf(VENTER, mapOf(
                    "hendelseId" to hendelse.id,
                    "hendelse" to hendelse.hendelse,
                    "vedtaksperiodeId" to vedtaksperiodeVenter.vedtaksperiodeId,
                    "fodselsnummer" to vedtaksperiodeVenter.fødselsnummer,
                    "organisasjonsnummer" to vedtaksperiodeVenter.organisasjonsnummer,
                    "ventetSiden" to vedtaksperiodeVenter.ventetSiden,
                    "venterTil" to vedtaksperiodeVenter.venterTil.coerceAtMost(LiksomMAX),
                    "venterPaVedtaksperiodeId" to vedtaksperiodeVenter.venterPå.vedtaksperiodeId,
                    "venterPaOrganisasjonsnummer" to vedtaksperiodeVenter.venterPå.organisasjonsnummer,
                    "venterPaHva" to vedtaksperiodeVenter.venterPå.hva,
                    "venterPaHvorfor" to vedtaksperiodeVenter.venterPå.hvorfor
                ))
            )
        }
    }

    internal fun venterIkke(vedtaksperiodeVentet: VedtaksperiodeVenter, hendelse: Hendelse) {
        return sessionOf(dataSource).use {session ->
            session.update(
                queryOf(VENTER_IKKE, mapOf(
                    "hendelseId" to hendelse.id,
                    "hendelse" to hendelse.hendelse,
                    "vedtaksperiodeId" to vedtaksperiodeVentet.vedtaksperiodeId,
                    "fodselsnummer" to vedtaksperiodeVentet.fødselsnummer,
                    "organisasjonsnummer" to vedtaksperiodeVentet.organisasjonsnummer
                ))
            )
        }
    }

    private companion object {
        @Language("PostgreSQL")
        val HENT_OM_VENTER = "SELECT * FROM vedtaksperiode_ventetilstand WHERE vedtaksperiodeId = :vedtaksperiodeId ORDER BY tidsstempel DESC LIMIT 1"

        @Language("PostgreSQL")
        val VENTER = """
            INSERT INTO vedtaksperiode_ventetilstand(hendelseId, hendelse, venter, vedtaksperiodeId, fodselsnummer, organisasjonsnummer, ventetSiden, venterTil, venterPaVedtaksperiodeId, venterPaOrganisasjonsnummer, venterPaHva, venterPaHvorfor)
            VALUES (:hendelseId, :hendelse::jsonb, true, :vedtaksperiodeId, :fodselsnummer, :organisasjonsnummer, :ventetSiden, :venterTil, :venterPaVedtaksperiodeId, :venterPaOrganisasjonsnummer, :venterPaHva, :venterPaHvorfor) 
        """

        @Language("PostgreSQL")
        val VENTER_IKKE = """
            INSERT INTO vedtaksperiode_ventetilstand(hendelseId, hendelse, venter, vedtaksperiodeId, fodselsnummer, organisasjonsnummer)
            VALUES (:hendelseId, :hendelse::jsonb, false, :vedtaksperiodeId, :fodselsnummer, :organisasjonsnummer)  
        """

        private val LiksomMAX = LocalDateTime.MAX.withYear(9999)

        val Row.vedtaksperiodeVenter get() = VedtaksperiodeVenter(
            vedtaksperiodeId = this.uuid("vedtaksperiodeId"),
            fødselsnummer = this.string("fodselsnummer"),
            organisasjonsnummer = this.string("organisasjonsnummer"),
            ventetSiden = this.localDateTime("ventetSiden"),
            venterTil = this.localDateTime("venterTil"),
            venterPå = VenterPå(
                vedtaksperiodeId = this.uuid("venterPaVedtaksperiodeId"),
                organisasjonsnummer = this.string("venterPaOrganisasjonsnummer"),
                hva = this.string("venterPaHva"),
                hvorfor = this.stringOrNull("venterPaHvorfor")
            )
        )
    }
}