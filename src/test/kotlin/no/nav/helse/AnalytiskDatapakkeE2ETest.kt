package no.nav.helse

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import java.time.LocalDateTime
import java.util.UUID
import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.E2eTestApp.Companion.e2eTest
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class AnalytiskDatapakkeE2ETest {

    @Test
    fun `lagrer analytisk datapakke i databasen`() = e2eTest {
        rapid.sendTestMessage(AnalytiskDatapakkeEvent())
        assertEquals(1, tellAnalytiskDatapakke())
    }

    @Test
    fun `sjekker at alle felter er i datapakken`() = e2eTest {
        val vedtaksperiodeId = UUID.randomUUID()
        val behandlingId = UUID.randomUUID()
        rapid.sendTestMessage(AnalytiskDatapakkeEvent(vedtaksperiodeId = vedtaksperiodeId, behandlingId = behandlingId))

        val lagretDatapakke = sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = """
                SELECT vedtaksperiode_id, behandling_id, datapakke FROM vedtaksdata 
                WHERE vedtaksperiode_id = :vedtaksperiodeId AND behandling_id = :behandlingId
            """
            requireNotNull(
                session.run(
                    queryOf(query, mapOf("vedtaksperiodeId" to vedtaksperiodeId, "behandlingId" to behandlingId))
                        .map { row -> listOf(
                            "vedtaksperiodeId" to row.string("vedtaksperiode_id"),
                            "behandlingId" to row.string("behandling_id"),
                            "datapakke" to row.string("datapakke"))
                        }
                        .asSingle
                )
            )
        }
        assertEquals(vedtaksperiodeId.toString(), lagretDatapakke[0].second)
        assertEquals(behandlingId.toString(), lagretDatapakke[1].second)
        assertTrue(areJsonStringsEqual(
            lagretDatapakke[2].second,
            forventetJson(vedtaksperiodeId, behandlingId)
        ))
    }

    private fun areJsonStringsEqual(json1: String, json2: String): Boolean {
        val objectMapper = ObjectMapper()
        val map1: Map<String, Any> = objectMapper.readValue(json1)
        val map2: Map<String, Any> = objectMapper.readValue(json2)
        return map1 == map2
    }

    private fun E2eTestApp.tellAnalytiskDatapakke(): Int {
        return sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT COUNT(*) FROM vedtaksdata"
            requireNotNull(
                session.run(queryOf(query).map { row -> row.int(1) }.asSingle)
            )
        }
    }

    @Language("JSON")
    private fun AnalytiskDatapakkeEvent(
        vedtaksperiodeId: UUID = UUID.randomUUID(),
        behandlingId: UUID = UUID.randomUUID()
    ): String {
        return """
        {
            "@event_name": "analytisk_datapakke",
            "@id": "${UUID.randomUUID()}",
            "@opprettet": "${LocalDateTime.now()}",
            "vedtaksperiodeId": "$vedtaksperiodeId",
            "behandlingId": "$behandlingId",
            "yrkesaktivitetstype": "ARBEIDSTAKER",
            "skjæringstidspunkt": "2025-01-01",
            "fom": "2025-01-01",
            "tom": "2025-01-31",
            "harAndreInntekterIBeregning": false,
            "beløpTilBruker": {
              "totalBeløp": 0, 
              "nettoBeløp": 0
            },
            "beløpTilArbeidsgiver": {
              "totalBeløp": 0,
              "nettoBeløp": 0
            },
            "antallGjenståendeSykedagerEtterPeriode": {
              "antallDager": 0,
              "nettoDager": 0
            },
            "antallForbrukteSykedagerEtterPeriode": {
              "antallDager": 0,
              "nettoDager": 0
            },
            "fødselsnummer": "211113442367"
        }
        """
    }

    @Language("JSON")
    private fun forventetJson(
        vedtaksperiodeId: UUID = UUID.randomUUID(),
        behandlingId: UUID = UUID.randomUUID()
    ): String {
        return """
        {
            "vedtaksperiodeId": "${vedtaksperiodeId}",
            "behandlingId": "${behandlingId}",
            "yrkesaktivitetstype": "ARBEIDSTAKER",
            "skjæringstidspunkt": "2025-01-01",
            "fom": "2025-01-01",
            "tom": "2025-01-31",
            "harAndreInntekterIBeregning": false,
            "beløpTilBruker": {
              "totalBeløp": 0, 
              "nettoBeløp": 0
            },
            "beløpTilArbeidsgiver": {
              "totalBeløp": 0,
              "nettoBeløp": 0
            },
            "antallGjenståendeSykedagerEtterPeriode": {
              "antallDager": 0,
              "nettoDager": 0
            },
            "antallForbrukteSykedagerEtterPeriode": {
              "antallDager": 0,
              "nettoDager": 0
            }
        }
        """
    }
}
