package no.nav.helse.ventetilstand

import kotliquery.*
import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.helse.ventetilstand.VedtaksperiodeVenter.Companion.vedtaksperiodeVenterMedMetadata
import org.intellij.lang.annotations.Language
import javax.sql.DataSource
import no.nav.helse.sikkerlogg
import no.nav.helse.ventetilstand.VedtaksperiodeVenter.Companion.vedtaksperiodeVenter

internal class VedtaksperiodeVentetilstandDao(private val dataSource: DataSource) {

    internal fun venter(fødselsnummer: String, vedtaksperiodeVenter: List<VedtaksperiodeVenter>, hendelse: Hendelse) {
        sessionOf(dataSource).use { session ->
            session.transaction { transaction ->
                val ventetFør = transaction.venter(fødselsnummer)
                val (bevar, insert) = vedtaksperiodeVenter.partition { it in ventetFør }
                transaction.venterIkke(fødselsnummer, bevar)
                insert.forEach { transaction.venter(fødselsnummer, it, hendelse) }

                val antallVentetFør = ventetFør.size
                val antallVenterNå = vedtaksperiodeVenter.size
                val antallNyInformasjon = insert.size

                when (listOf(antallVentetFør, antallVenterNå, antallNyInformasjon).all { it == 0 }) {
                    true -> { /* Skulle ønske vi kunne returne her, men da rollbacket transactionen 🤔 */ }
                    false -> {
                        sikkerlogg.info("Personen med {} venter i systemet. VentetFør=$antallVentetFør, VenterNå=$antallVenterNå, NyInformasjon=$antallNyInformasjon", keyValue("fødselsnummer", fødselsnummer))
                    }
                }
            }
        }
    }

    private fun TransactionalSession.venter(fødselsnummer: String) =
        list(queryOf(PERSON_VENTER, mapOf("fodselsnummer" to fødselsnummer))) { row ->
            row.vedtaksperiodeVenter
        }

    private fun TransactionalSession.venter(fødselsnummer: String, vedtaksperiodeVenter: VedtaksperiodeVenter, hendelse: Hendelse) {
        execute(queryOf(VENTER, mapOf(
            "hendelseId" to hendelse.id,
            "hendelse" to hendelse.hendelse,
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

    private fun TransactionalSession.venterIkke(fødselsnummer: String, bevar: List<VedtaksperiodeVenter>) {
        val statement = when (bevar.isEmpty()) {
            true -> "DELETE FROM vedtaksperiode_venter WHERE fodselsnummer = '$fødselsnummer'"
            false -> "DELETE FROM vedtaksperiode_venter WHERE fodselsnummer = '$fødselsnummer' AND vedtaksperiodeId NOT IN (${bevar.joinToString { "'${it.vedtaksperiodeId}'" }})"
        }
        execute(queryOf(statement))
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
        private val PERSON_VENTER = "SELECT * FROM vedtaksperiode_venter WHERE fodselsnummer = :fodselsnummer"

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
