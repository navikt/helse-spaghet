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
class VedtaksperiodeGodkjentE2ETest {
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
    fun `lagrer warnings i database`() {
        val vedtaksperiodeId = UUID.randomUUID()
        river.sendTestMessage(godkjenning(vedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeGodkjent(vedtaksperiodeId))
        assertEquals(listOf("Arbeidsuførhet, aktivitetsplikt og/eller medvirkning må vurderes. Se forklaring på vilkårs-siden.", "Perioden er lagt inn i Infotrygd, men ikke utbetalt. Fjern fra Infotrygd hvis det utbetales via speil."), hentWarnings(vedtaksperiodeId))
    }

    @Test
    fun `assosieres med en godkjenning i database`() {
        val vedtaksperiodeId = UUID.randomUUID()
        val vedtaksperiodeId2 = UUID.randomUUID()
        river.sendTestMessage(godkjenning(vedtaksperiodeId))
        river.sendTestMessage(godkjenning(vedtaksperiodeId2))
        river.sendTestMessage(vedtaksperiodeGodkjent(vedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeGodkjent(vedtaksperiodeId2))
        assertEquals(listOf("Arbeidsuførhet, aktivitetsplikt og/eller medvirkning må vurderes. Se forklaring på vilkårs-siden.", "Perioden er lagt inn i Infotrygd, men ikke utbetalt. Fjern fra Infotrygd hvis det utbetales via speil."), hentWarnings(vedtaksperiodeId))
    }

    @Test
    fun `lagrer inntektskilde i database`() {
        val vedtaksperiodeId = UUID.randomUUID()
        river.sendTestMessage(godkjenning(vedtaksperiodeId))
        assertEquals("EN_ARBEIDSGIVER", finnInntektskilde(vedtaksperiodeId))
    }

    private fun hentWarnings(vedtaksperiodeId: UUID) : List<String> =
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = """
                SELECT w.* FROM warning_for_godkjenning w
                 JOIN GODKJENNING g ON g.id = w.godkjenning_ref
                 WHERE g.vedtaksperiode_id = :vedtaksperiodeId
                ;
                """.trimIndent()
            return session.run(queryOf(query, mapOf("vedtaksperiodeId" to vedtaksperiodeId))
                .map { it.string("melding") }
                .asList
            )
        }

    private fun finnInntektskilde(vedtaksperiodeId: UUID) = sessionOf(dataSource).use { session ->
        session.run(queryOf("SELECT * FROM godkjenning WHERE vedtaksperiode_id=?;", vedtaksperiodeId)
            .map { it.stringOrNull("inntektskilde") }
            .asSingle)
    }


    @Language("JSON")
    private fun vedtaksperiodeGodkjent(vedtaksperiodeId: UUID) = """
    {
      "@event_name": "vedtaksperiode_godkjent",
      "@opprettet": "2021-01-01T01:01:01.721176",
      "@id": "6e9ffa77-a25f-44a4-42bc-cc88c6610c2e",
      "fødselsnummer": "1234567901",
      "vedtaksperiodeId": "$vedtaksperiodeId",
      "warnings": [
        {
          "melding": "Arbeidsuførhet, aktivitetsplikt og/eller medvirkning må vurderes. Se forklaring på vilkårs-siden.",
          "kilde": "Spesialist"
        },
        {
          "melding": "Perioden er lagt inn i Infotrygd, men ikke utbetalt. Fjern fra Infotrygd hvis det utbetales via speil.",
          "kilde": "Spleis"
        }
      ],
      "periodetype": "OVERGANG_FRA_IT",
      "saksbehandlerIdent": "F123456",
      "saksbehandlerEpost": "siri.saksbehandler@nav.no",
      "automatiskBehandling": false,
      "system_read_count": 0,
      "system_participating_services": [
        {
          "service": "spesialist",
          "instance": "spesialist-7dfb74c754-w7rsk",
          "time": "2021-01-01T01:01:38.721232"
        }
      ]
    }
    

    """

    @Language("JSON")
    private fun godkjenning(vedtaksperiodeId: UUID) = """{
  "@behov": [
    "Godkjenning"
  ],
  "@løsning": {
    "Godkjenning": {
      "godkjent": true,
      "saksbehandlerIdent": "Automatisk behandlet",
      "saksbehandlerEpost": "tbd@nav.no",
      "godkjenttidspunkt": "2021-02-10T13:26:02.502304",
      "automatiskBehandling": true,
      "årsak": null,
      "begrunnelser": null,
      "kommentar": null,
      "makstidOppnådd": false
    }
  },
  "Godkjenning": {
    "periodetype": "INFOTRYGDFORLENGELSE",
    "inntektskilde": "EN_ARBEIDSGIVER",
    "warnings": {
      "aktiviteter": [],
      "kontekster": []
    }
  },
  "vedtaksperiodeId": "$vedtaksperiodeId",
  "aktørId": "1234",
  "fødselsnummer": "1234567901"
}
    """
}
