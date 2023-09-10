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
import java.util.UUID
import javax.sql.DataSource

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal abstract class AbstractVedtaksperiodeVentetilstandTest(
    configureDb: (container: PostgreSQLContainer<Nothing>) -> Pair<DataSource, FluentConfiguration> = { setupFlyway(it) }
) {
    private val embeddedPostgres = embeddedPostgres()
    private val db = configureDb(embeddedPostgres)
    private val dataSource = db.first
    protected val flyway = db.second
    protected val vedtaksperiodeVentetilstandDao = VedtaksperiodeVentetilstandDao(dataSource)

    protected val river = TestRapid().apply {
        VedtaksperiodeVenterRiver(this, dataSource)
        VedtaksperiodeEndretRiver(this, dataSource)
        PersonAvstemmingRiver(this, dataSource)
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
        fødselsnummer: String = "11111111111"
    ) = """
        {
          "@event_name": "vedtaksperiode_venter",
          "organisasjonsnummer": "123456789",
          "vedtaksperiodeId": "$vedtaksperiodeId",
          "ventetSiden": "2023-03-04T21:34:17.96322",
          "venterTil": "+999999999-12-31T23:59:59.999999999",
          "venterPå": {
            "vedtaksperiodeId": "$venterPåVedtaksperiodeId",
            "organisasjonsnummer": "987654321",
            "venteårsak": {
              "hva": "$venterPå",
              "hvorfor": "TESTOLINI"
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
    protected fun personAvstemt(vedtaksperiodeId: UUID, hendelseId: UUID = UUID.randomUUID(), fødselsnummer: String = "11111111111") = """
         {
          "@event_name": "person_avstemt",
          "arbeidsgivere": [
            {
              "organisasjonsnummer": "123456789",
              "forkastedeVedtaksperioder": [
                {
                  "id": "$vedtaksperiodeId",
                  "tilstand": "TIL_INFOTRYGD"
                }
              ]
            }
          ],
          "@id": "$hendelseId",
          "fødselsnummer": "$fødselsnummer",
          "aktørId": "aktørId"
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
    protected val antallRader get() = sessionOf(dataSource).use { session ->
        session.run(queryOf("SELECT count(*) FROM vedtaksperiode_ventetilstand").map { row -> row.int(1) }.asSingle)
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
    protected fun hentGjeldende(): Map<UUID, String> {
        @Language("PostgreSQL")
        val SQL = """
            SELECT vedtaksperiodeId, fodselsnummer FROM vedtaksperiode_ventetilstand
            WHERE gjeldende = true
        """

        return sessionOf(dataSource).use { session ->
            session.list(queryOf(SQL)) { row ->
                row.uuid("vedtaksperiodeId") to row.string("fodselsnummer")
            }
        }.toMap()
    }

    protected fun hentVedtaksperiodeIderSomVenter() =
        hentDeSomVenter().map { it.vedtaksperiodeId }.toSet()
}