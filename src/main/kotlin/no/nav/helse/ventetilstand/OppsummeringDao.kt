package no.nav.helse.ventetilstand

import kotliquery.Query
import kotliquery.sessionOf
import org.intellij.lang.annotations.Language
import javax.sql.DataSource

internal class OppsummeringDao(private val dataSource: DataSource) {

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

    private companion object {

        @Language("PostgreSQL")
        private val OPPSUMMERING = """
            SELECT concat(TRIM(TRAILING '_FORDI_' FROM concat(venterpahva, '_FORDI_', venterpahvorfor)), case when date_part('Year', ventertil) = 9999 AND ventetSiden <= (now() AT TIME ZONE 'Europe/Oslo') - INTERVAL '3 MONTHS' then '_SOM_IKKE_KAN_FORKASTES_OG_HAR_VENTET_MER_ENN_3_MÅNEDER' else '' end) as arsak, vedtaksperiodeid = venterpavedtaksperiodeid as propp, count(1) as antall
            FROM vedtaksperiode_venter
            WHERE tidsstempel < now() - INTERVAL '5 MINUTES' -- Mulig de bare er i transit
            GROUP BY arsak, propp
            ORDER BY antall DESC
        """

        @Language("PostgreSQL")
        private val OPPSUMMERING_EXTERNAL = """
            with siste as (
                select venterpahva, ventetsiden, (now()::date - ventetsiden::date) as ventetIAntallDager
                from vedtaksperiode_venter
                where tidsstempel < now() - INTERVAL '5 MINUTES' -- Mulig de bare er i transit
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
            FROM vedtaksperiode_venter
            WHERE tidsstempel < now() - INTERVAL '5 MINUTES' -- Mulig de bare er i transit
        """
    }

}