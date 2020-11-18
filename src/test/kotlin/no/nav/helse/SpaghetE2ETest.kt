package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SpaghetE2ETest {
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
    fun `godkjenningsbehov blir lest fra rapid`() {
        val fødselsnummer = "1243356"
        val vedtaksperiodeId = UUID.randomUUID()
        river.sendTestMessage(løsning(fødselsnummer, vedtaksperiodeId, "FORLENGELSE"))

        assertEquals(listOf(vedtaksperiodeId.toString()), finnGodkjenninger(fødselsnummer))
    }

    @Test
    fun `godkjenningsbehov blir lest fra rapid når periodetype er null`() {
        val fødselsnummer = "6543210"
        val vedtaksperiodeId = UUID.randomUUID()
        river.sendTestMessage(løsning(fødselsnummer, vedtaksperiodeId, null))

        assertEquals(listOf(vedtaksperiodeId.toString()), finnGodkjenninger(fødselsnummer))
    }

    private fun finnGodkjenninger(fødselsnummer: String) = using(sessionOf(dataSource)) { session ->
        session.run(queryOf("SELECT * FROM godkjenning WHERE fodselsnummer=?;", fødselsnummer)
            .map { it.string("vedtaksperiode_id") }
            .asList)
    }

    @Language("JSON")
    private fun løsning(fødselsnummer: String, vedtaksperiodeId: UUID, periodetype: String?) = """
        {
          "@event_name": "behov",
          "@opprettet": "2020-06-02T12:00:00.000000",
          "@id": "7e0187b7-07cf-4246-8ae9-4b642bf871a3",
          "@behov": [
            "Godkjenning"
          ],
          "aktørId": "1000000000000",
          "fødselsnummer": "$fødselsnummer",
          "organisasjonsnummer": "987654321",
          "vedtaksperiodeId": "$vedtaksperiodeId",
          "tilstand": "AVVENTER_GODKJENNING",
          "periodeFom": "2020-05-16",
          "periodeTom": "2020-05-22",
          "sykepengegrunnlag": 42069.0,
          "warnings": {
            "aktiviteter": [
              {
                "kontekster": [],
                "alvorlighetsgrad": "WARN",
                "melding": "Perioden er en direkte overgang fra periode i Infotrygd",
                "detaljer": {},
                "tidsstempel": "2020-06-02 15:56:34.111"
              }
            ],
            "kontekster": []
          },
          ${periodetype?.let { "\"periodetype\": \"$it\"," } ?: ""}
          "@løsning": {
            "Godkjenning": {
              "godkjent": true,
              "saksbehandlerIdent": "Z999999",
              "godkjenttidspunkt": "2020-06-02T13:00:00.000000"
            }
          },
          "@final": true,
          "@besvart": "2020-06-02T13:00:00.000000"
        }
    """
}
