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
internal class VarselE2ETest {
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
    fun `lagrer varsel`() {
        river.sendTestMessage(aktivitetsloggNyAktivitet())
        assertEquals(1, tellVarsel())
    }

    private fun tellVarsel(): Int {
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