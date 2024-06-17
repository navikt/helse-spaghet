package no.nav.helse.ventetilstand

import kotliquery.*
import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.helse.ventetilstand.VedtaksperiodeVenter.Companion.vedtaksperiodeVenter
import org.intellij.lang.annotations.Language
import org.slf4j.LoggerFactory
import java.util.*
import javax.sql.DataSource

internal class GjeldendeVedtaksperiodeVentetilstandDao(private val dataSource: DataSource): VedtaksperiodeVentetilstandDao {

    override fun venter(vedtaksperiodeVenter: VedtaksperiodeVenter, hendelse: Hendelse) {
        sessionOf(dataSource).use { session ->
            session.transaction { transaction ->
                val vedtaksperiodeId = vedtaksperiodeVenter.vedtaksperiodeId
                if (transaction.hent(vedtaksperiodeId) == vedtaksperiodeVenter) return logger.info("Ingen endring på ventetilstand for {}", keyValue("vedtaksperiodeId", vedtaksperiodeId))
                transaction.venterIkke(vedtaksperiodeVenter.vedtaksperiodeId) // Sletter eventuell eksisterende rad
                transaction.execute(queryOf(VENTER, mapOf(
                    "hendelseId" to hendelse.id,
                    "hendelse" to hendelse.hendelse,
                    "vedtaksperiodeId" to vedtaksperiodeVenter.vedtaksperiodeId,
                    "skjaeringstidspunkt" to vedtaksperiodeVenter.skjæringstidspunkt,
                    "fodselsnummer" to vedtaksperiodeVenter.fødselsnummer,
                    "organisasjonsnummer" to vedtaksperiodeVenter.organisasjonsnummer,
                    "ventetSiden" to vedtaksperiodeVenter.ventetSiden,
                    "venterTil" to vedtaksperiodeVenter.venterTil,
                    "venterForAlltid" to (vedtaksperiodeVenter.venterTil.year == 9999),
                    "venterPaVedtaksperiodeId" to vedtaksperiodeVenter.venterPå.vedtaksperiodeId,
                    "venterPaSkjaeringstidspunkt" to vedtaksperiodeVenter.venterPå.skjæringstidspunkt,
                    "venterPaOrganisasjonsnummer" to vedtaksperiodeVenter.venterPå.organisasjonsnummer,
                    "venterPaHva" to vedtaksperiodeVenter.venterPå.hva,
                    "venterPaHvorfor" to vedtaksperiodeVenter.venterPå.hvorfor
                )))
                logger.info("Lagret ny ventetilstand for {}", keyValue("vedtaksperiodeId", vedtaksperiodeId))
            }
        }
    }

    private fun Session.venterIkke(vedtaksperiodeId: UUID) = execute(queryOf(VENTER_IKKE, mapOf("vedtaksperiodeId" to vedtaksperiodeId)))

    override fun venterIkke(vedtaksperiodeId: UUID, hendelse: Hendelse) {
        sessionOf(dataSource).use { session -> session.venterIkke(vedtaksperiodeId) }
    }

    internal fun stuck() = sessionOf(dataSource).use { session ->
        session.list(Query(STUCK)) { row ->
            row.vedtaksperiodeVenter
        }
    }

    private fun TransactionalSession.hent(vedtaksperiodeId: UUID) =
        single(queryOf(HENT_OM_VENTER, mapOf("vedtaksperiodeId" to vedtaksperiodeId))) { row -> row.vedtaksperiodeVenter }

    private companion object {
        private val logger = LoggerFactory.getLogger(GjeldendeVedtaksperiodeVentetilstandDao::class.java)

        @Language("PostgreSQL")
        private val HENT_OM_VENTER = "SELECT * FROM vedtaksperiode_venter WHERE vedtaksperiodeId = :vedtaksperiodeId"

        @Language("PostgreSQL")
        private val VENTER = """
            INSERT INTO vedtaksperiode_venter(hendelseId, hendelse, vedtaksperiodeId, skjaeringstidspunkt, fodselsnummer, organisasjonsnummer, ventetSiden, venterTil, venterForAlltid, venterPaVedtaksperiodeId, venterPaSkjaeringstidspunkt, venterPaOrganisasjonsnummer, venterPaHva, venterPaHvorfor)
            VALUES (:hendelseId, :hendelse::jsonb, :vedtaksperiodeId, :skjaeringstidspunkt, :fodselsnummer, :organisasjonsnummer, :ventetSiden, :venterTil, :venterForAlltid, :venterPaVedtaksperiodeId, :venterPaSkjaeringstidspunkt, :venterPaOrganisasjonsnummer, :venterPaHva, :venterPaHvorfor) 
        """

        @Language("PostgreSQL")
        private val VENTER_IKKE = "DELETE FROM vedtaksperiode_venter WHERE vedtaksperiodeId = :vedtaksperiodeId"

        @Language("PostgreSQL")
        private val STUCK = """
            SELECT * FROM vedtaksperiode_venter
            -- Må ha ventet minst 5 minutter før vi anser det som stuck
            WHERE ventetSiden < (now() AT TIME ZONE 'Europe/Oslo') - INTERVAL '5 MINUTES'
            AND (
                -- Om vi venter på en av disse tingene skal det alltid være alarm
                (venterPaHva in ('BEREGNING', 'UTBETALING', 'HJELP'))
                    OR
                -- Om vi venter for alltid skal alarmen gå når vi har ventet 3 måneder, så fremt det ikke venter på godkjenning fra saksbehandler eller inntektsmelding
                (venterForAlltid = true AND ventetSiden < (now() AT TIME ZONE 'Europe/Oslo') - INTERVAL '3 MONTHS' AND venterPaHva not in ('GODKJENNING', 'INNTEKTSMELDING'))
                    OR 
                -- Om venterTil (maksdato) er nådd så tyder det på at vi er en periode som ikke kan forkastes tross at maksdato er nådd.
                -- Trekker fra ekstra 10 dager for å være sikker på at den ikke bare venter på å få en påminnelse fra Spock før den forkastes
                (venterForAlltid = false AND ventertil < (now() AT TIME ZONE 'Europe/Oslo') - INTERVAL '10 DAYS')
            )
        """
    }
}