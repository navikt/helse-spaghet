package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.E2eTestApp.Companion.e2eTest
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.*

internal class VarselE2ETest {
    @Test
    fun `lagrer varsel`() = e2eTest {
        rapid.sendTestMessage(aktivitetsloggNyAktivitet())
        assertEquals(1, tellVarsel())
    }

    private fun E2eTestApp.tellVarsel(): Int {
        return sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT COUNT(*) FROM regelverksvarsel"
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
              "nivå": "VARSEL",
              "melding": "Yrkesskade oppgitt i søknaden",
              "kontekster": [
                {
                  "konteksttype": "Vedtaksperiode",
                  "kontekstmap": {
                    "vedtaksperiodeId": "${UUID.randomUUID()}"
                  }
                }
              ],
              "varselkode": "RV_YS_1"
            }
        ],
        "@opprettet": "2023-05-08T10:18:42.439435518"
        }
    """
}