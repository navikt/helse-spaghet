package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.E2eTestApp.Companion.e2eTest
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.lang.IllegalStateException
import java.util.*

class VedtaksperiodeGodkjentE2ETest {
    @Test
    fun `lagrer varsel i database`() = e2eTest {
        val vedtaksperiodeId = UUID.randomUUID()
        rapid.sendTestMessage(godkjenning(vedtaksperiodeId))
        rapid.sendTestMessage(varselEndret(vedtaksperiodeId, varseltittel = "En tittel"))
        assertEquals(listOf("En tittel"), hentVarsler(vedtaksperiodeId))
    }

    @Test
    fun `lagrer varsel uten behandlingId i database`() = e2eTest {
        val vedtaksperiodeId = UUID.randomUUID()
        rapid.sendTestMessage(varselEndretUtenBehandlingId(vedtaksperiodeId, varseltittel = "En tittel"))
        assertEquals(listOf("En tittel"), hentVarsler(vedtaksperiodeId))
    }

    @Test
    fun `ugyldig varselkode kaster exception`() = e2eTest {
        val vedtaksperiodeId = UUID.randomUUID()

        assertDoesNotThrow {
            rapid.sendTestMessage(varselEndret(vedtaksperiodeId, varselkode = "SB_EX_1"))
            rapid.sendTestMessage(varselEndret(vedtaksperiodeId, varselkode = "RV_SY_1"))
        }

        assertThrows<IllegalStateException> {
            rapid.sendTestMessage(varselEndret(vedtaksperiodeId, varselkode = "EN_KODE"))
        }
    }

    @Test
    fun `lagrer varsel for avvisning i database`() = e2eTest {
        val vedtaksperiodeId = UUID.randomUUID()
        rapid.sendTestMessage(varselEndret(vedtaksperiodeId, varseltittel = "En tittel"))
        assertEquals(listOf("En tittel"), hentVarsler(vedtaksperiodeId))
    }

    @Test
    fun `lagrer inntektskilde i database`() = e2eTest {
        val vedtaksperiodeId = UUID.randomUUID()
        rapid.sendTestMessage(godkjenning(vedtaksperiodeId))
        assertEquals("EN_ARBEIDSGIVER", finnInntektskilde(vedtaksperiodeId))
    }

    @Test
    fun `lagrer utbetalingtype i database`() = e2eTest {
        val vedtaksperiodeId = UUID.randomUUID()
        rapid.sendTestMessage(godkjenning(vedtaksperiodeId))
        assertEquals("UTBETALING", finnUtbetalingType(vedtaksperiodeId))
    }

    @Test
    fun `lagrer refusjontype i database`() = e2eTest {
        val vedtaksperiodeId = UUID.randomUUID()
        rapid.sendTestMessage(godkjenning(vedtaksperiodeId))
        assertEquals("FULL_REFUSJON", finnRefusjonType(vedtaksperiodeId))
    }

    @Test
    fun `lagrer saksbehandleroverstyringer i database`() = e2eTest {
        val vedtaksperiodeId = UUID.randomUUID()
        val saksbehandleroverstyringer = listOf(UUID.randomUUID(), UUID.randomUUID())
        rapid.sendTestMessage(godkjenning(vedtaksperiodeId, saksbehandleroverstyringer))
        assertTrue(finnErSaksbehandleroverstyringer(vedtaksperiodeId)!!)
        assertEquals(saksbehandleroverstyringer, finnGodkjenningOverstyringer(vedtaksperiodeId))
    }

    private fun E2eTestApp.hentVarsler(vedtaksperiodeId: UUID) : List<String> =
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = """
                SELECT * FROM varsel WHERE vedtaksperiode_id = :vedtaksperiodeId
                """
            return session.run(queryOf(query, mapOf("vedtaksperiodeId" to vedtaksperiodeId))
                .map { it.string("tittel") }
                .asList
            )
        }

    private fun E2eTestApp.finnInntektskilde(vedtaksperiodeId: UUID) = sessionOf(dataSource).use { session ->
        session.run(queryOf("SELECT * FROM godkjenning WHERE vedtaksperiode_id=?;", vedtaksperiodeId)
            .map { it.stringOrNull("inntektskilde") }
            .asSingle)
    }

    private fun E2eTestApp.finnErSaksbehandleroverstyringer(vedtaksperiodeId: UUID) = sessionOf(dataSource).use { session ->
        session.run(queryOf("SELECT * FROM godkjenning WHERE vedtaksperiode_id=?;", vedtaksperiodeId)
            .map {
                it.boolean("er_saksbehandleroverstyringer")
            }
            .asSingle)
    }

    private fun E2eTestApp.finnGodkjenningOverstyringer(vedtaksperiodeId: UUID) = sessionOf(dataSource).use { session ->
        session.run(queryOf("""
            SELECT * FROM godkjenning_overstyringer go
            INNER JOIN godkjenning g ON g.id = go.godkjenning_ref
            WHERE g.vedtaksperiode_id=?;""".trimMargin(), vedtaksperiodeId
        )
        .map { it.uuid("overstyring_hendelse_id") }
        .asList)
    }

    private fun E2eTestApp.finnUtbetalingType(vedtaksperiodeId: UUID) = sessionOf(dataSource).use { session ->
        session.run(queryOf("SELECT * FROM godkjenning WHERE vedtaksperiode_id=?;", vedtaksperiodeId)
            .map { it.stringOrNull("utbetaling_type") }
            .asSingle)
    }


    private fun E2eTestApp.finnRefusjonType(vedtaksperiodeId: UUID) = sessionOf(dataSource).use { session ->
        session.run(queryOf("SELECT * FROM godkjenning WHERE vedtaksperiode_id=?;", vedtaksperiodeId)
            .map { it.stringOrNull("refusjon_type") }
            .asSingle)
    }

    @Language("JSON")
    private fun varselEndret(vedtaksperiodeId: UUID, varseltittel: String = "En tittel", varselkode: String = "SB_EX_1") = """
       {
         "@event_name": "varsel_endret", 
         "@opprettet": "2021-05-27T12:20:09.12384379", 
         "@id": "a38fec49-2d1d-402e-832c-6017a71e625f", 
         "fødselsnummer": "12345678901",
         "vedtaksperiode_id": "$vedtaksperiodeId",
         "varseltittel": "$varseltittel",
         "varselkode": "$varselkode",
         "forrige_status": "VURDERT",
         "gjeldende_status": "GODKJENT",
         "varsel_id": "${UUID.randomUUID()}",
         "behandling_id": "${UUID.randomUUID()}"
       }
    """

    @Language("JSON")
    private fun varselEndretUtenBehandlingId(vedtaksperiodeId: UUID, varseltittel: String = "En tittel", varselkode: String = "SB_EX_1") = """
       {
         "@event_name": "varsel_endret", 
         "@opprettet": "2021-05-27T12:20:09.12384379", 
         "@id": "a38fec49-2d1d-402e-832c-6017a71e625f", 
         "fødselsnummer": "12345678901",
         "vedtaksperiode_id": "$vedtaksperiodeId",
         "varseltittel": "$varseltittel",
         "varselkode": "$varselkode",
         "forrige_status": "VURDERT",
         "gjeldende_status": "GODKJENT",
         "varsel_id": "${UUID.randomUUID()}"
       }
    """

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
    "behandlingId":  "$behandlingId",
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
