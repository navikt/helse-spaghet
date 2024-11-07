package no.nav.helse

import com.github.navikt.tbd_libs.rapids_and_rivers.test_support.TestRapid
import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.E2eTestApp.Companion.e2eTest
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.*

class SkatteinntekterLagtTilGrunnE2ETest {
    @Test
    fun `lagrer i databasen`() = e2eTest {
        rapid.sendTestMessage(skatteinntekterLagtTilGrunnEvent())
        assertEquals(1, tellSkatteinntekterLagtTilGrunn())
    }

    private fun E2eTestApp.tellSkatteinntekterLagtTilGrunn(): Int {
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