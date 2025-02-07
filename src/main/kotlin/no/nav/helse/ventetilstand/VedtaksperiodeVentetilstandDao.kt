package no.nav.helse.ventetilstand

import kotliquery.*
import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.helse.ventetilstand.VedtaksperiodeVenter.Companion.vedtaksperiodeVenterMedMetadata
import org.intellij.lang.annotations.Language
import java.util.*
import javax.sql.DataSource
import no.nav.helse.sikkerlogg

internal class VedtaksperiodeVentetilstandDao(private val dataSource: DataSource) {

    internal fun venter(fødselsnummer: String, vedtaksperiodeVenter: List<VedtaksperiodeVenter>, hendelse: Hendelse) {
        sessionOf(dataSource).use { session ->
            session.transaction { transaction ->
                transaction.venterIkke(fødselsnummer)
                vedtaksperiodeVenter.forEach { transaction.venter(fødselsnummer, it, hendelse) }
                sikkerlogg.info("Personen med {} har ${vedtaksperiodeVenter.size} perioder som venter", keyValue("fødselsnummer", fødselsnummer))
            }
        }
    }

    private fun TransactionalSession.venter(fødselsnummer: String, vedtaksperiodeVenter: VedtaksperiodeVenter, hendelse: Hendelse) {
        execute(queryOf(VENTER, mapOf(
            "hendelseId" to hendelse.id,
            "hendelse" to hendelse.hendelse, // TODO: Dette med hendelser er jo rimelig uinteressant nå. Slette?
            "vedtaksperiodeId" to vedtaksperiodeVenter.vedtaksperiodeId,
            "skjaeringstidspunkt" to vedtaksperiodeVenter.skjæringstidspunkt,
            "fodselsnummer" to fødselsnummer,
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
    }

    private fun TransactionalSession.venterIkke(fødselsnummer: String) {
        execute(queryOf(PERSON_VENTER_IKKE, mapOf("fodselsnummer" to fødselsnummer)))
    }

    private fun Session.venterIkke(vedtaksperiodeId: UUID) = execute(queryOf(VENTER_IKKE, mapOf("vedtaksperiodeId" to vedtaksperiodeId)))

    internal fun venterIkke(vedtaksperiodeId: UUID, hendelse: Hendelse) {
        sessionOf(dataSource).use { session -> session.venterIkke(vedtaksperiodeId) }
    }

    internal fun stuck() = sessionOf(dataSource).use { session ->
        session.list(Query(STUCK)) { row ->
            row.vedtaksperiodeVenterMedMetadata
        }
    }

    private companion object {
        @Language("PostgreSQL")
        private val VENTER = """
            INSERT INTO vedtaksperiode_venter(hendelseId, hendelse, vedtaksperiodeId, skjaeringstidspunkt, fodselsnummer, organisasjonsnummer, ventetSiden, venterTil, venterForAlltid, venterPaVedtaksperiodeId, venterPaSkjaeringstidspunkt, venterPaOrganisasjonsnummer, venterPaHva, venterPaHvorfor)
            VALUES (:hendelseId, :hendelse::jsonb, :vedtaksperiodeId, :skjaeringstidspunkt, :fodselsnummer, :organisasjonsnummer, :ventetSiden, :venterTil, :venterForAlltid, :venterPaVedtaksperiodeId, :venterPaSkjaeringstidspunkt, :venterPaOrganisasjonsnummer, :venterPaHva, :venterPaHvorfor) 
        """

        @Language("PostgreSQL")
        private val VENTER_IKKE = "DELETE FROM vedtaksperiode_venter WHERE vedtaksperiodeId = :vedtaksperiodeId"

        @Language("PostgreSQL")
        private val PERSON_VENTER_IKKE = "DELETE FROM vedtaksperiode_venter WHERE fodselsnummer = :fodselsnummer"

        @Language("PostgreSQL")
        private val STUCK = """
            SELECT * FROM vedtaksperiode_venter
                -- Må ha ventet minst 5 minutter før vi anser det som stuck. Disse er mest sannsynlig i transit.
                -- 'ventetSiden' er sist gang vedtaksperioden endret tilstand, så i tilstandene AVVENTER_BLOKKERENDE & AVENTER_REVURDERING blir det ofte feil å ta utgangspunkt i det tidspunktet.
                -- F.eks. kan en periode vært i AVVENTER_BLOKKERENDE i 2 måneder og ventet på Godkjenning. Om perioden foran overstyres, og denne spørringen
                -- kjøres mens vi venter på Utbetaling så vil perioden melde om at den har ventet 2 måneder på på utbetaling.
                -- Sjekker heller om de er i transit basert på når raden er lagret i Spaghet. Derfor viktig at vi ikke lager ny rad (med nytt tidsstempel) om vi får
                -- ny melding som sier at vi venter på akkurat det samme som før
            WHERE tidsstempel < now() - INTERVAL '5 MINUTES'
            AND (
                -- Om vi venter på en av disse tingene skal det alltid være alarm
                (venterPaHva in ('BEREGNING', 'UTBETALING', 'HJELP'))
                -- Om hvorfor er satt til noe skal det være alarm så fremt det ikke er på grunn av overstyring igangsatt
                    OR
                (venterPaHvorfor IS NOT NULL AND venterPaHvorfor != 'OVERSTYRING_IGANGSATT')
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
