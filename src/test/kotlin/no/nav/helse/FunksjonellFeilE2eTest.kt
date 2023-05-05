package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FunksjonellFeilE2eTest {
    private val embeddedPostgres = embeddedPostgres()
    private val dataSource = setupDataSourceMedFlyway(embeddedPostgres)
    private val river = TestRapid()
        .setupRivers(dataSource)

    @AfterAll
    fun tearDown() {
        river.stop()
        dataSource.connection.close()
        embeddedPostgres.close()
    }

    @Test
    fun `lagrer funksjonelle feil`() {
        river.sendTestMessage(aktivitetsloggNyAktivitet())
        assertEquals(1, tellFunksjonellFeil())
    }

    private fun tellFunksjonellFeil(): Int {
        return sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT COUNT(*) FROM funksjonell_feil"
            requireNotNull(
                session.run(queryOf(query).map { row -> row.int(1) }.asSingle)
            )
        }
    }

    private fun aktivitetsloggNyAktivitet() = """
        {
          "@event_name": "aktivitetslogg_ny_aktivitet",
          "aktiviteter": [
            {
              "nivå": "FUNKSJONELL_FEIL",
              "melding": "Har mer enn 25 % avvik",
              "kontekster": [
                {
                  "konteksttype": "Vedtaksperiode",
                  "kontekstmap": {
                    "vedtaksperiodeId": "${UUID.randomUUID()}"
                  }
                }
              ],
              "varselkode": "RV_IV_2"
            }
          ],
          "@opprettet": "2023-05-05T13:22:49.709928663"
        }
    """
}