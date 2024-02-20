package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.Util.uuid
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import no.nav.helse.ventetilstand.*
import no.nav.helse.ventetilstand.VedtaksperiodeVentetilstandDao.Companion.vedtaksperiodeVenter
import org.flywaydb.core.api.configuration.FluentConfiguration
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.PostgreSQLContainer
import java.time.LocalDateTime
import java.util.UUID
import javax.sql.DataSource

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal abstract class AbstractVedtaksperiodeVentetilstandTest(
    configureDb: (container: PostgreSQLContainer<Nothing>) -> Pair<DataSource, FluentConfiguration> = { setupFlyway(it) }
) {
    private val embeddedPostgres = embeddedPostgres()
    private val db = configureDb(embeddedPostgres)
    private val dataSource = db.first
    protected val vedtaksperiodeVentetilstandDao = VedtaksperiodeVentetilstandDao(dataSource)

    protected val river = TestRapid().apply {
        VedtaksperiodeVenterRiver(this, dataSource)
        VedtaksperiodeVenterIkkeRiver(this, dataSource)
        VedtaksperiodeEndretRiver(this, dataSource)
    }

    @AfterAll
    fun tearDown() {
        river.stop()
        dataSource.connection.close()
        embeddedPostgres.close()
    }

    @BeforeEach
    fun reset() {
        river.reset()
        dataSource.resetDatabase()
    }

    @Language("JSON")
    protected fun vedtaksperiodeVenter(
        vedtaksperiodeId: UUID,
        venterPåVedtaksperiodeId: UUID,
        venterPå: String = "GODKJENNING",
        hendelseId: UUID = UUID.randomUUID(),
        fødselsnummer: String = "11111111111",
        venterPåHvorfor: String = "TESTOLINI",
        ventetSiden: LocalDateTime = LocalDateTime.parse("2023-03-04T21:34:17.96322")
    ) = """
        {
          "@event_name": "vedtaksperiode_venter",
          "organisasjonsnummer": "123456789",
          "vedtaksperiodeId": "$vedtaksperiodeId",
          "skjæringstidspunkt": "2019-01-01",
          "ventetSiden": "$ventetSiden",
          "venterTil": "+999999999-12-31T23:59:59.999999999",
          "venterPå": {
            "vedtaksperiodeId": "$venterPåVedtaksperiodeId",
            "skjæringstidspunkt": "2018-01-01",
            "organisasjonsnummer": "987654321",
            "venteårsak": {
              "hva": "$venterPå",
              "hvorfor": "$venterPåHvorfor"
            }
          },
          "@id": "$hendelseId",
          "fødselsnummer": "$fødselsnummer"
        }
    """

    @Language("JSON")
    protected fun vedtaksperiodeEndret(vedtaksperiodeId: UUID, hendelseId: UUID = UUID.randomUUID(), fødselsnummer: String = "11111111111") = """
         {
          "@event_name": "vedtaksperiode_endret",
          "organisasjonsnummer": "123456789",
          "vedtaksperiodeId": "$vedtaksperiodeId",
          "gjeldendeTilstand": "AVVENTER_INNTEKTSMELDING",
          "forrigeTilstand": "AVVENTER_INFOTRYGDHISTORIKK",
          "@id": "$hendelseId",
          "fødselsnummer": "$fødselsnummer"
        } 
    """

    @Language("JSON")
    protected fun vedtaksperiodeVenterIkke(vedtaksperiodeId: UUID, hendelseId: UUID = UUID.randomUUID()) = """
         {
          "@event_name": "vedtaksperiode_venter_ikke",
          "vedtaksperiodeId": "$vedtaksperiodeId",
          "@id": "$hendelseId"
        } 
    """

    protected fun hendelseIderFor(vedtaksperiodeId: UUID) = sessionOf(dataSource).use { session ->
        session.list(
            queryOf(
                "SELECT hendelseId FROM vedtaksperiode_ventetilstand WHERE vedtaksperiodeId = :vedtaksperiodeId", mapOf(
                    "vedtaksperiodeId" to vedtaksperiodeId
                )
            )
        ) { it.uuid("hendelseId") }.toSet()
    }

    protected fun hentDeSomVenter(): Set<VedtaksperiodeVenter> {
        val venterBasertPåTimestamp = hentDeSomVenterBasertPåTimestamp()
        val venterBasertPåGjeldende = hentDeSomVenterBasertPåGjeldende()
        assertEquals(venterBasertPåTimestamp, venterBasertPåGjeldende) { "De som venter basert på timestamp og gjeldende er ikke like!" }
        return venterBasertPåGjeldende
    }

    private fun hentDeSomVenterBasertPåTimestamp(): Set<VedtaksperiodeVenter> {
        @Language("PostgreSQL")
        val SQL = """
            WITH sistePerVedtaksperiodeId AS (
                SELECT DISTINCT ON (vedtaksperiodeId) *
                FROM vedtaksperiode_ventetilstand
                ORDER BY vedtaksperiodeId, tidsstempel DESC
            )
            SELECT * FROM sistePerVedtaksperiodeId
            WHERE venter = true
            ORDER BY ventetSiden ASC
        """

        return sessionOf(dataSource).use { session ->
            session.list(queryOf(SQL)) { row ->
                row.vedtaksperiodeVenter
            }
        }.toSet()
    }
    private fun hentDeSomVenterBasertPåGjeldende(): Set<VedtaksperiodeVenter> {
        @Language("PostgreSQL")
        val SQL = """
            SELECT * FROM vedtaksperiode_ventetilstand
            WHERE gjeldende = true
            AND venter = true
        """

        return sessionOf(dataSource).use { session ->
            session.list(queryOf(SQL)) { row ->
                row.vedtaksperiodeVenter
            }
        }.toSet()
    }

    protected fun hentVedtaksperiodeIderSomVenter() =
        hentDeSomVenter().map { it.vedtaksperiodeId }.toSet()
}