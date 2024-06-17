package no.nav.helse.ventetilstand

import kotliquery.Query
import kotliquery.queryOf
import kotliquery.sessionOf
import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.helse.ventetilstand.VedtaksperiodeVenter.Companion.vedtaksperiodeVenter
import org.intellij.lang.annotations.Language
import org.slf4j.LoggerFactory
import java.util.*
import javax.sql.DataSource

internal class HistoriskVedtaksperiodeVentetilstandDao(private val dataSource: DataSource): VedtaksperiodeVentetilstandDao {

    override fun venter(vedtaksperiodeVenter: VedtaksperiodeVenter, hendelse: Hendelse) {
        val vedtaksperiodeId = vedtaksperiodeVenter.vedtaksperiodeId
        if (hentOmVenter(vedtaksperiodeId) == vedtaksperiodeVenter) return logger.info("Ingen endring på ventetilstand for {}", keyValue("vedtaksperiodeId", vedtaksperiodeId))
        nyGjeldende(vedtaksperiodeVenter, hendelse, queryOf(VENTER, mapOf(
            "hendelseId" to hendelse.id,
            "hendelse" to hendelse.hendelse,
            "vedtaksperiodeId" to vedtaksperiodeVenter.vedtaksperiodeId,
            "skjaeringstidspunkt" to vedtaksperiodeVenter.skjæringstidspunkt,
            "fodselsnummer" to vedtaksperiodeVenter.fødselsnummer,
            "organisasjonsnummer" to vedtaksperiodeVenter.organisasjonsnummer,
            "ventetSiden" to vedtaksperiodeVenter.ventetSiden,
            "venterTil" to vedtaksperiodeVenter.venterTil,
            "venterPaVedtaksperiodeId" to vedtaksperiodeVenter.venterPå.vedtaksperiodeId,
            "venterPaSkjaeringstidspunkt" to vedtaksperiodeVenter.venterPå.skjæringstidspunkt,
            "venterPaOrganisasjonsnummer" to vedtaksperiodeVenter.venterPå.organisasjonsnummer,
            "venterPaHva" to vedtaksperiodeVenter.venterPå.hva,
            "venterPaHvorfor" to vedtaksperiodeVenter.venterPå.hvorfor
        )))
        logger.info("Lagret ny ventetilstand for {}", keyValue("vedtaksperiodeId", vedtaksperiodeId))
    }

    override fun venterIkke(vedtaksperiodeId: UUID, hendelse: Hendelse) {
        val vedtaksperiodeVentet = hentOmVenter(vedtaksperiodeId) ?: return
        nyGjeldende(vedtaksperiodeVentet, hendelse, queryOf(VENTER_IKKE, mapOf(
            "hendelseId" to hendelse.id,
            "hendelse" to hendelse.hendelse,
            "vedtaksperiodeId" to vedtaksperiodeVentet.vedtaksperiodeId,
            "skjaeringstidspunkt" to vedtaksperiodeVentet.skjæringstidspunkt,
            "fodselsnummer" to vedtaksperiodeVentet.fødselsnummer,
            "organisasjonsnummer" to vedtaksperiodeVentet.organisasjonsnummer
        )))
    }

    internal fun hentOmVenter(vedtaksperiodeId: UUID): VedtaksperiodeVenter? {
        return sessionOf(dataSource).use { session ->
            session.single(
                queryOf(HENT_OM_VENTER, mapOf("vedtaksperiodeId" to vedtaksperiodeId))
            ){ row -> row.vedtaksperiodeVenter }
        }
    }

    private fun nyGjeldende(vedtaksperiodeVenter: VedtaksperiodeVenter, hendelse: Hendelse, nyGjeldendeQuery: Query) {
        sessionOf(dataSource).use { session ->
            session.transaction {
                it.execute(queryOf(IKKE_LENGER_GJELDENDE, mapOf("vedtaksperiodeId" to vedtaksperiodeVenter.vedtaksperiodeId, "hendelseId" to hendelse.id)))
                it.execute(nyGjeldendeQuery)
            }
        }
    }

    internal data class Ventegruppe(val årsak: String, val antall: Int, val propp: Boolean)
    internal fun oppsummering() = sessionOf(dataSource).use { session ->
        val oppsummering = session.list(Query(OPPSUMMERING)) { row ->
            Ventegruppe(årsak = row.string("arsak"), propp = row.boolean("propp"), antall = row.int("antall"))
        }

        val antallPersoner = session.single(Query(ANTALL_PERSONER_SOM_VENTER)) { row ->
            row.int("antallPersoner")
        } ?: 0
        oppsummering to antallPersoner
    }

    internal data class VentegruppeExternal(val årsak: String, val antall: Int, val bucket: String)

    internal fun oppsummeringExternal(): List<VentegruppeExternal> {
        return sessionOf(dataSource).use { session ->
            session.list(Query(OPPSUMMERING_EXTERNAL)) { row ->
                VentegruppeExternal(årsak = row.string("venter_på"), antall = row.int("antall"), bucket = row.string("ventet_i"))
            }
        }
    }

    internal companion object {
        private val logger = LoggerFactory.getLogger(HistoriskVedtaksperiodeVentetilstandDao::class.java)

        @Language("PostgreSQL")
        private val HENT_OM_VENTER = "SELECT * FROM vedtaksperiode_ventetilstand WHERE vedtaksperiodeId = :vedtaksperiodeId AND gjeldende = true AND venter = true"

        @Language("PostgreSQL")
        private val VENTER = """
            INSERT INTO vedtaksperiode_ventetilstand(hendelseId, hendelse, venter, vedtaksperiodeId, skjaeringstidspunkt, fodselsnummer, organisasjonsnummer, ventetSiden, venterTil, venterPaVedtaksperiodeId, venterPaSkjaeringstidspunkt, venterPaOrganisasjonsnummer, venterPaHva, venterPaHvorfor, gjeldende)
            VALUES (:hendelseId, :hendelse::jsonb, true, :vedtaksperiodeId, :skjaeringstidspunkt, :fodselsnummer, :organisasjonsnummer, :ventetSiden, :venterTil, :venterPaVedtaksperiodeId, :venterPaSkjaeringstidspunkt, :venterPaOrganisasjonsnummer, :venterPaHva, :venterPaHvorfor, true) 
            ON CONFLICT (hendelseId) DO NOTHING
        """

        @Language("PostgreSQL")
        private val VENTER_IKKE = """
            INSERT INTO vedtaksperiode_ventetilstand(hendelseId, hendelse, venter, vedtaksperiodeId, skjaeringstidspunkt, fodselsnummer, organisasjonsnummer, gjeldende)
            VALUES (:hendelseId, :hendelse::jsonb, false, :vedtaksperiodeId, :skjaeringstidspunkt, :fodselsnummer, :organisasjonsnummer, true)
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
        private val OPPSUMMERING = """
            SELECT concat(TRIM(TRAILING '_FORDI_' FROM concat(venterpahva, '_FORDI_', venterpahvorfor)), case when date_part('Year', ventertil) = 9999 AND ventetSiden <= (now() AT TIME ZONE 'Europe/Oslo') - INTERVAL '3 MONTHS' then '_SOM_IKKE_KAN_FORKASTES_OG_HAR_VENTET_MER_ENN_3_MÅNEDER' else '' end) as arsak, vedtaksperiodeid = venterpavedtaksperiodeid as propp, count(1) as antall
            FROM vedtaksperiode_ventetilstand
            WHERE venter = true AND gjeldende = true AND ventetSiden < (now() AT TIME ZONE 'Europe/Oslo') - INTERVAL '5 MINUTES' -- Mulig de bare er i transit
            GROUP BY arsak, propp
            ORDER BY antall DESC
        """

        @Language("PostgreSQL")
        private val OPPSUMMERING_EXTERNAL = """
            with siste as (
                select venterpahva, ventetsiden, (now()::date - vedtaksperiode_ventetilstand.ventetsiden::date) as ventetIAntallDager
                from vedtaksperiode_ventetilstand
                where venter=true AND gjeldende=true
            )
            select count(*) as antall,
                   (case when venterpahva = 'INNTEKTSMELDING' THEN 'INFORMASJON FRA ARBEIDSGIVER' WHEN venterpahva = 'GODKJENNING' THEN 'SAKSBEHANDLER' ELSE venterpahva end) as venter_på,
                   (CASE WHEN ventetIAntallDager > 90 THEN 'OVER 90 DAGER' WHEN ventetIAntallDager > 30 THEN 'MELLOM 30 OG 90 DAGER' ELSE 'UNDER 30 DAGER' end) as ventet_i,
                   (CASE WHEN ventetIAntallDager > 90 THEN 3 WHEN ventetIAntallDager > 30 THEN 2 ELSE 1 end) as sortering
            from siste
            group by venter_på, ventet_i, sortering
            having count(*) > 10
            order by venter_på, sortering
        """


        @Language("PostgreSQL")
        private val ANTALL_PERSONER_SOM_VENTER = """
            SELECT count(distinct fodselsnummer) as antallPersoner
            FROM vedtaksperiode_ventetilstand WHERE venter = true AND gjeldende = true
            AND ventetSiden < (now() AT TIME ZONE 'Europe/Oslo') - INTERVAL '5 MINUTES'  -- Mulig de bare er i transit

        """
    }
}