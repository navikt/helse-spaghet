package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FunksjonellFeilE2ETest {
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

    @AfterEach
    fun slettRader() {
        return sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "TRUNCATE TABLE funksjonell_feil"
            session.run(queryOf(query).asExecute)
        }
    }

    @Test
    fun `lagrer funksjonelle feil`() {
        river.sendTestMessage(aktivitetsloggNyAktivitet())
        assertEquals(1, tellFunksjonellFeil())
    }

    @Test
    fun `lagrer ikke duplikate funksjonelle feil`() {
        river.sendTestMessage(aktivitetsloggNyAktivitetMedDuplikater())
        assertEquals(1, tellFunksjonellFeil())
    }

    @Test
    fun `håndterer aktiviteter uten vedtaksperiode i kontektsen`() {
        river.sendTestMessage(aktivitetsloggNyAktivitetUtenVedtaksperiodeId())
        assertEquals(0, tellFunksjonellFeil())
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
                },
                {
                  "konteksttype": "Person",
                  "kontekstmap": {}
                }
              ],
              "varselkode": "RV_IV_2"
            }
          ],
          "@opprettet": "2023-05-05T13:22:49.709928663"
        }
    """

    private fun aktivitetsloggNyAktivitetMedDuplikater(vedtaksperiodeId: UUID = UUID.randomUUID()) = """
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
                    "vedtaksperiodeId": "$vedtaksperiodeId"
                  }
                },
                {
                  "konteksttype": "Person",
                  "kontekstmap": {}
                }
              ],
              "varselkode": "RV_IV_2"
            },
            {
              "nivå": "FUNKSJONELL_FEIL",
              "melding": "Har mer enn 25 % avvik",
              "kontekster": [
                {
                  "konteksttype": "Vedtaksperiode",
                  "kontekstmap": {
                    "vedtaksperiodeId": "$vedtaksperiodeId"
                  }
                }
              ],
              "varselkode": "RV_IV_2"
            }
          ],
          "@opprettet": "2023-05-05T13:22:49.709928663"
        }
    """

    private fun aktivitetsloggNyAktivitetUtenVedtaksperiodeId() = """
        {
          "@event_name": "aktivitetslogg_ny_aktivitet",
          "aktiviteter": [
            {
              "nivå": "FUNKSJONELL_FEIL",
              "melding": "Har mer enn 25 % avvik",
              "kontekster": [
                {
                  "konteksttype": "Person",
                  "kontekstmap": {}
                }
              ],
              "varselkode": "RV_IV_2"
            }
          ],
          "@opprettet": "2023-05-05T13:22:49.709928663"
        }
    """
}