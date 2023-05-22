package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.Util.uuid
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
        .setupRivers(dataSource)

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
    fun `lagrer warnings for avvisning i database`() {
        val vedtaksperiodeId = UUID.randomUUID()
        river.sendTestMessage(godkjenning(vedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeAvvist(vedtaksperiodeId))
        assertEquals(listOf("Bruker har mottatt AAP innenfor 6 måneder av skjæringstidspunkt. Kontroller at brukeren har rett til sykepenger", "Det finnes åpne oppgaver på sykepenger i Gosys"), hentWarnings(vedtaksperiodeId))
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

    @Test
    fun `lagrer utbetalingtype i database`() {
        val vedtaksperiodeId = UUID.randomUUID()
        river.sendTestMessage(godkjenning(vedtaksperiodeId))
        assertEquals("UTBETALING", finnUtbetalingType(vedtaksperiodeId))
    }

    @Test
    fun `lagrer refusjontype i database`() {
        val vedtaksperiodeId = UUID.randomUUID()
        river.sendTestMessage(godkjenning(vedtaksperiodeId))
        assertEquals("FULL_REFUSJON", finnRefusjonType(vedtaksperiodeId))
    }

    @Test
    fun `lagrer saksbehandleroverstyringer i database`() {
        val vedtaksperiodeId = UUID.randomUUID()
        val saksbehandleroverstyringer = listOf(UUID.randomUUID(), UUID.randomUUID())
        river.sendTestMessage(godkjenning(vedtaksperiodeId, saksbehandleroverstyringer))
        assertTrue(finnErSaksbehandleroverstyringer(vedtaksperiodeId)!!)
        assertEquals(saksbehandleroverstyringer, finnGodkjenningOverstyringer(vedtaksperiodeId))
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

    private fun finnErSaksbehandleroverstyringer(vedtaksperiodeId: UUID) = sessionOf(dataSource).use { session ->
        session.run(queryOf("SELECT * FROM godkjenning WHERE vedtaksperiode_id=?;", vedtaksperiodeId)
            .map {
                it.boolean("er_saksbehandleroverstyringer")
            }
            .asSingle)
    }

    private fun finnGodkjenningOverstyringer(vedtaksperiodeId: UUID) = sessionOf(dataSource).use { session ->
        session.run(queryOf("""
            SELECT * FROM godkjenning_overstyringer go
            INNER JOIN godkjenning g ON g.id = go.godkjenning_ref
            WHERE g.vedtaksperiode_id=?;""".trimMargin(), vedtaksperiodeId
        )
        .map { it.uuid("overstyring_hendelse_id") }
        .asList)
    }

    private fun finnUtbetalingType(vedtaksperiodeId: UUID) = sessionOf(dataSource).use { session ->
        session.run(queryOf("SELECT * FROM godkjenning WHERE vedtaksperiode_id=?;", vedtaksperiodeId)
            .map { it.stringOrNull("utbetaling_type") }
            .asSingle)
    }


    private fun finnRefusjonType(vedtaksperiodeId: UUID) = sessionOf(dataSource).use { session ->
        session.run(queryOf("SELECT * FROM godkjenning WHERE vedtaksperiode_id=?;", vedtaksperiodeId)
            .map { it.stringOrNull("refusjon_type") }
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
    private fun vedtaksperiodeAvvist(vedtaksperiodeId: UUID) = """{
  "@event_name": "vedtaksperiode_avvist",
  "@opprettet": "2021-05-27T12:20:09.12384379",
  "@id": "4b47c841-aa08-4cab-b13a-1fefd2024b24",
  "fødselsnummer": "12345678901",
  "vedtaksperiodeId": "$vedtaksperiodeId",
  "warnings": [
    {
      "melding": "Bruker har mottatt AAP innenfor 6 måneder av skjæringstidspunkt. Kontroller at brukeren har rett til sykepenger",
      "kilde": "Spleis"
    },
    {
      "melding": "Det finnes åpne oppgaver på sykepenger i Gosys",
      "kilde": "Spesialist"
    }
  ],
  "saksbehandlerIdent": "V12345",
  "saksbehandlerEpost": "mille.mellomleder@nav.no",
  "automatiskBehandling": false,
  "årsak": "Feil vurdering og/eller beregning",
  "begrunnelser": ["No shirt, no shoes, no service"],
  "kommentar": "",
  "periodetype": "OVERGANG_FRA_IT",
  "system_read_count": 0,
  "system_participating_services": [
    {
      "service": "spesialist",
      "instance": "spesialist-1234567890a-skpjd",
      "time": "2021-02-13T11:27:29.123456789"
    }
  ]
}"""

    @Language("JSON")
    private fun godkjenning(
        vedtaksperiodeId: UUID,
        saksbehandleroverstyringer: List<UUID> = emptyList(),
        behandlingId: UUID = UUID.randomUUID()
    ) = """{
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
      "makstidOppnådd": false,
      "refusjontype": "FULL_REFUSJON",
      "saksbehandleroverstyringer": [${saksbehandleroverstyringer.joinToString { """"$it"""" }}]
    }
  },
  "Godkjenning": {
    "periodetype": "INFOTRYGDFORLENGELSE",
    "inntektskilde": "EN_ARBEIDSGIVER",
    "utbetalingtype": "UTBETALING",
    "warnings": {
      "aktiviteter": [],
      "kontekster": []
    }
  },
  "behandlingId": "$behandlingId",
  "vedtaksperiodeId": "$vedtaksperiodeId",
  "aktørId": "1234",
  "fødselsnummer": "1234567901"
}
    """
}
