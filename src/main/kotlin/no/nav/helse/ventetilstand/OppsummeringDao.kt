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
            SELECT
                concat(
                    concat(venterpahva, CASE WHEN venterpahvorfor IS NOT NULL THEN '_FORDI_' END, venterpahvorfor),
                    CASE WHEN venterForAlltid = true AND ventetSiden <= (now() AT TIME ZONE 'Europe/Oslo') - INTERVAL '3 MONTHS' THEN '_SOM_IKKE_KAN_FORKASTES_OG_HAR_VENTET_MER_ENN_3_MÅNEDER' END
                ) AS arsak,
                vedtaksperiodeid = venterpavedtaksperiodeid AS propp, 
                count(1) AS antall
            FROM vedtaksperiode_venter
            WHERE tidsstempel < now() - INTERVAL '5 MINUTES' -- Mulig de bare er i transit
            GROUP BY arsak, propp
            ORDER BY antall DESC
        """

        @Language("PostgreSQL")
        private val OPPSUMMERING_EXTERNAL = """
            WITH siste AS (
                SELECT venterpahva, ventetsiden, (now()::date - ventetsiden::date) AS ventetIAntallDager
                FROM vedtaksperiode_venter
                WHERE tidsstempel < now() - INTERVAL '5 MINUTES' -- Mulig de bare er i transit
            )
            SELECT count(1) AS antall,
                   (CASE WHEN venterpahva = 'INNTEKTSMELDING' THEN 'INFORMASJON FRA ARBEIDSGIVER' WHEN venterpahva = 'GODKJENNING' THEN 'SAKSBEHANDLER' ELSE venterpahva END) AS venter_på,
                   (CASE WHEN ventetIAntallDager > 90 THEN 'OVER 90 DAGER' WHEN ventetIAntallDager > 30 THEN 'MELLOM 30 OG 90 DAGER' ELSE 'UNDER 30 DAGER' END) AS ventet_i,
                   (CASE WHEN ventetIAntallDager > 90 THEN 3 WHEN ventetIAntallDager > 30 THEN 2 ELSE 1 END) AS sortering
            FROM siste
            GROUP BY venter_på, ventet_i, sortering
            HAVING count(1) > 10
            ORDER BY venter_på, sortering
        """

        @Language("PostgreSQL")
        private val ANTALL_PERSONER_SOM_VENTER = """
            SELECT count(distinct fodselsnummer) AS antallPersoner
            FROM vedtaksperiode_venter
            WHERE tidsstempel < now() - INTERVAL '5 MINUTES' -- Mulig de bare er i transit
        """
    }

}