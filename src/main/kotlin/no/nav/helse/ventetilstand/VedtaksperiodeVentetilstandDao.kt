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
            -- Må ha ventet minst 5 minutter før vi anser det som stuck
            AND ventetSiden < (now() AT TIME ZONE 'Europe/Oslo') - INTERVAL '5 MINUTES'
            AND (
                -- Om vi venter på en av disse tingene skal det alltid være alarm
                (venterPaHva in ('BEREGNING', 'UTBETALING', 'HJELP'))
                    OR
                -- Om vi ikke har noe makstid skal alarmen gå når vi har ventet 3 måneder, så fremt det ikke venter på godkjenning fra saksbehandler eller inntektsmelding
                (date_part('Year', ventertil) = 9999 AND ventetSiden < (now() AT TIME ZONE 'Europe/Oslo') - INTERVAL '3 MONTHS' AND venterPaHva not in ('GODKJENNING', 'INNTEKTSMELDING'))
                    OR 
                -- Om maksdato er nådd så tyder det på at vi er en periode som ikke kan forkastes tross at maksdato er nådd.
                -- Trekker fra ekstra 10 dager for å være sikker på at den ikke bare venter på å få en påminnelse fra Spock før den forkastes
                (date_part('Year', ventertil) != 9999 AND ventertil < (now() AT TIME ZONE 'Europe/Oslo') - INTERVAL '10 DAYS')
            )
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
        private val ANTALL_PERSONER_SOM_VENTER = """
            SELECT count(distinct fodselsnummer) as antallPersoner
            FROM vedtaksperiode_ventetilstand WHERE venter = true AND gjeldende = true
            AND ventetSiden < (now() AT TIME ZONE 'Europe/Oslo') - INTERVAL '5 MINUTES'  -- Mulig de bare er i transit

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