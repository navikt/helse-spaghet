package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.E2eTestApp.Companion.e2eTest
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.*

class FunksjonellFeilE2ETest {
    @Test
    fun `lagrer funksjonelle feil`() =
        e2eTest {
            rapid.sendTestMessage(aktivitetsloggNyAktivitet())
            assertEquals(1, tellFunksjonellFeil())
        }

    @Test
    fun `lagrer ikke duplikate funksjonelle feil`() =
        e2eTest {
            rapid.sendTestMessage(aktivitetsloggNyAktivitetMedDuplikater())
            assertEquals(1, tellFunksjonellFeil())
        }

    @Test
    fun `håndterer aktiviteter uten vedtaksperiode i kontektsen`() =
        e2eTest {
            rapid.sendTestMessage(aktivitetsloggNyAktivitetUtenVedtaksperiodeId())
            assertEquals(0, tellFunksjonellFeil())
        }

    private fun E2eTestApp.tellFunksjonellFeil(): Int =
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT COUNT(*) FROM funksjonell_feil"
            requireNotNull(
                session.run(queryOf(query).map { row -> row.int(1) }.asSingle),
            )
        }

    private fun aktivitetsloggNyAktivitet() =
        """
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

    private fun aktivitetsloggNyAktivitetMedDuplikater(vedtaksperiodeId: UUID = UUID.randomUUID()) =
        """
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

    private fun aktivitetsloggNyAktivitetUtenVedtaksperiodeId() =
        """
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
