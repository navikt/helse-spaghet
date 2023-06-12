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
             ){ row -> row.vedtaksperiodeVenter }
        }
    }

    internal fun venter(vedtaksperiodeVenter: VedtaksperiodeVenter, hendelse: Hendelse) {
        nyGjeldende(vedtaksperiodeVenter, hendelse, queryOf(VENTER, mapOf(
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
        )))
    }

    internal fun venterIkke(vedtaksperiodeVentet: VedtaksperiodeVenter, hendelse: Hendelse) {
        nyGjeldende(vedtaksperiodeVentet, hendelse, queryOf(VENTER_IKKE, mapOf(
            "hendelseId" to hendelse.id,
            "hendelse" to hendelse.hendelse,
            "vedtaksperiodeId" to vedtaksperiodeVentet.vedtaksperiodeId,
            "fodselsnummer" to vedtaksperiodeVentet.fødselsnummer,
            "organisasjonsnummer" to vedtaksperiodeVentet.organisasjonsnummer
        )))
    }

    private fun nyGjeldende(vedtaksperiodeVenter: VedtaksperiodeVenter, hendelse: Hendelse, nyGjeldendeQuery: Query) {
        sessionOf(dataSource).use { session ->
            session.transaction {
                it.execute(queryOf(IKKE_LENGER_GJELDENDE, mapOf("vedtaksperiodeId" to vedtaksperiodeVenter.vedtaksperiodeId, "hendelseId" to hendelse.id)))
                it.execute(nyGjeldendeQuery)
            }
        }
    }

    internal fun stuck() = sessionOf(dataSource).use { session ->
        session.list(Query(STUCK)) { row ->
            row.vedtaksperiodeVenter
        }
    }

    internal companion object {
        @Language("PostgreSQL")
        private val HENT_OM_VENTER = "SELECT * FROM vedtaksperiode_ventetilstand WHERE vedtaksperiodeId = :vedtaksperiodeId AND gjeldende = true AND venter = true"

        @Language("PostgreSQL")
        private val VENTER = """
            INSERT INTO vedtaksperiode_ventetilstand(hendelseId, hendelse, venter, vedtaksperiodeId, fodselsnummer, organisasjonsnummer, ventetSiden, venterTil, venterPaVedtaksperiodeId, venterPaOrganisasjonsnummer, venterPaHva, venterPaHvorfor, gjeldende)
            VALUES (:hendelseId, :hendelse::jsonb, true, :vedtaksperiodeId, :fodselsnummer, :organisasjonsnummer, :ventetSiden, :venterTil, :venterPaVedtaksperiodeId, :venterPaOrganisasjonsnummer, :venterPaHva, :venterPaHvorfor, true) 
            ON CONFLICT (hendelseId) DO NOTHING
        """

        @Language("PostgreSQL")
        private val VENTER_IKKE = """
            INSERT INTO vedtaksperiode_ventetilstand(hendelseId, hendelse, venter, vedtaksperiodeId, fodselsnummer, organisasjonsnummer, gjeldende)
            VALUES (:hendelseId, :hendelse::jsonb, false, :vedtaksperiodeId, :fodselsnummer, :organisasjonsnummer, true)
            ON CONFLICT (hendelseId) DO NOTHING 
        """

        @Language("PostgreSQL")
        private val IKKE_LENGER_GJELDENDE = """
            UPDATE vedtaksperiode_ventetilstand
            SET gjeldende = false
            WHERE vedtaksperiodeId = :vedtaksperiodeId
            AND gjeldende = true
            AND hendelseId != :hendelseId
        """

        @Language("PostgreSQL")
        private val STUCK = """
            SELECT * FROM vedtaksperiode_ventetilstand
            WHERE gjeldende = true
            AND venter = true
            AND ventetSiden < (now() AT TIME ZONE 'Europe/Oslo') - INTERVAL '5 MINUTES'
            AND (
                (venterPaHva in ('BEREGNING', 'UTBETALING', 'HJELP'))
                    OR
                (date_part('Year', ventertil) = 9999 AND ventetSiden < (now() AT TIME ZONE 'Europe/Oslo') - INTERVAL '3 MONTHS' AND venterPaHva != 'GODKJENNING')
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