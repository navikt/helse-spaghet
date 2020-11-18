package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AktivitetRiverE2ETest {
    private val vedtaksperiodeId = UUID.randomUUID()
    private val embeddedPostgres = embeddedPostgres()
    private val dataSource = setupDataSourceMedFlyway(embeddedPostgres)
    private val river = TestRapid()
            .setupRiver(dataSource)

    @AfterAll
    fun tearDown() {
        river.stop()
        dataSource.connection.close()
        embeddedPostgres.close()
    }

    @Test
    fun `lagrer errors i database`() {
        river.sendTestMessage(vedtaksperiodeEndretMedError)

        assertEquals(listOf("Utbetaling skal gå rett til bruker"), hentErrors(vedtaksperiodeId))
    }

    private fun hentErrors(vedtaksperiodeId: UUID) =
            sessionOf(dataSource).use { session ->
                @Language("PostgreSQL")
                val query = """SELECT * FROM vedtaksperiode_aktivitet WHERE vedtaksperiode_id=?;"""
                session.run(queryOf(query, vedtaksperiodeId)
                        .map { it.string("melding") }
                        .asList
                )
            }

    @Language("JSON")
    private val vedtaksperiodeEndretMedError = """
{
  "vedtaksperiodeId": "$vedtaksperiodeId",
  "organisasjonsnummer": "987654321",
  "gjeldendeTilstand": "TIL_INFOTRYGD",
  "forrigeTilstand": "AVVENTER_GAP",
  "aktivitetslogg": {
    "aktiviteter": [
      {
        "alvorlighetsgrad": "ERROR",
        "melding": "Utbetaling skal gå rett til bruker",
        "tidsstempel": "2020-10-15 13:37:10.555"
      }
    ]
  },
  "@event_name": "vedtaksperiode_endret",
  "@id": "afaa7641-8215-4940-a984-cbefec7ac705",
  "@opprettet": "2020-10-15T13:37:10.55555",
  "aktørId": "1000000000096",
  "fødselsnummer": "10101010100"
}
    """
}
