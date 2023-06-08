package no.nav.helse.ventetilstand

import kotliquery.Query
import kotliquery.Row
import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.Util.uuid
import org.intellij.lang.annotations.Language
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
                    "venterTil" to vedtaksperiodeVenter.venterTil,
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

    internal fun stuck() = sessionOf(dataSource).use { session ->
        session.list(Query(STUCK)) { row ->
            row.vedtaksperiodeVenter
        }
    }

    internal companion object {
        @Language("PostgreSQL")
        private val HENT_OM_VENTER = "SELECT * FROM vedtaksperiode_ventetilstand WHERE vedtaksperiodeId = :vedtaksperiodeId ORDER BY tidsstempel DESC LIMIT 1"

        @Language("PostgreSQL")
        private val VENTER = """
            INSERT INTO vedtaksperiode_ventetilstand(hendelseId, hendelse, venter, vedtaksperiodeId, fodselsnummer, organisasjonsnummer, ventetSiden, venterTil, venterPaVedtaksperiodeId, venterPaOrganisasjonsnummer, venterPaHva, venterPaHvorfor)
            VALUES (:hendelseId, :hendelse::jsonb, true, :vedtaksperiodeId, :fodselsnummer, :organisasjonsnummer, :ventetSiden, :venterTil, :venterPaVedtaksperiodeId, :venterPaOrganisasjonsnummer, :venterPaHva, :venterPaHvorfor) 
            ON CONFLICT (hendelseId) DO NOTHING
        """

        @Language("PostgreSQL")
        private val VENTER_IKKE = """
            INSERT INTO vedtaksperiode_ventetilstand(hendelseId, hendelse, venter, vedtaksperiodeId, fodselsnummer, organisasjonsnummer)
            VALUES (:hendelseId, :hendelse::jsonb, false, :vedtaksperiodeId, :fodselsnummer, :organisasjonsnummer)
            ON CONFLICT (hendelseId) DO NOTHING 
        """

        @Language("PostgreSQL")
        private val STUCK = """
            WITH sistePerVedtaksperiodeId AS (
                SELECT DISTINCT ON (vedtaksperiodeId) *
                FROM vedtaksperiode_ventetilstand
                ORDER BY vedtaksperiodeId, tidsstempel DESC
            )
            SELECT * FROM sistePerVedtaksperiodeId
            WHERE venter = true
            AND (
                (venterPaHva in ('BEREGNING', 'UTBETALING', 'HJELP'))
                    OR
                (date_part('Year', ventertil) = 9999 AND ventetSiden < now() - INTERVAL '3 MONTHS' AND venterPaHva != 'GODKJENNING')
            )
            AND (
                (venterPaHvorfor is null)
                    OR
                (venterPaHvorfor not in ('VIL_UTBETALES', 'ALLEREDE_UTBETALT', 'VIL_AVSLUTTES'))
            )
        """

        internal val Row.vedtaksperiodeVenter get() = VedtaksperiodeVenter.opprett(
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