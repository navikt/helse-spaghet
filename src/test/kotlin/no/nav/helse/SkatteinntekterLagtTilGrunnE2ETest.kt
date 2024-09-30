package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SkatteinntekterLagtTilGrunnE2ETest {
    private val embeddedPostgres = embeddedPostgres()
    private val dataSource = setupDataSourceMedFlyway(embeddedPostgres)
    private val river = TestRapid()
        .setupRivers(dataSource)

    @Test
    fun `lagrer i databasen`() {
        river.sendTestMessage(skatteinntekterLagtTilGrunnEvent())
        assertEquals(1, tellSkatteinntekterLagtTilGrunn())
    }

    private fun tellSkatteinntekterLagtTilGrunn(): Int {
        return sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT COUNT(*) FROM skatteinntekter_lagt_til_grunn"
            requireNotNull(
                session.run(queryOf(query).map { row -> row.int(1) }.asSingle)
            )
        }
    }

    @Language("JSON")
    private fun skatteinntekterLagtTilGrunnEvent(vedtaksperiodeId: UUID = UUID.randomUUID()): String {
        return """
       {
         "@event_name": "skatteinntekter_lagt_til_grunn",
         "organisasjonsnummer": "987654321",
         "vedtaksperiodeId": "$vedtaksperiodeId",
         "behandlingId": "264ef682-d276-48d0-9f41-2a9dac711175",
         "omregnetÅrsinntekt": 372000.0,
         "skatteinntekter": [
           {
             "måned": "2017-10",
             "beløp": 31000.0
           },
           {
             "måned": "2017-11",
             "beløp": 31000.0
           },
           {
             "måned": "2017-12",
             "beløp": 31000.0
           }
         ],
         "@id": "dfa2ebc9-1ee8-4dc1-9f2e-e63d0b96fb5b",
         "@opprettet": "2024-09-27T13:07:25.898735",
         "aktørId": "42",
         "fødselsnummer": "12029240045"
       }
       """
    }

}