package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.Util.uuid
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import no.nav.helse.ventetilstand.*
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.LocalDateTime
import java.util.UUID

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class VedtaksperiodeVentetilstandTest {
    private val embeddedPostgres = embeddedPostgres()
    private val dataSource = setupDataSourceMedFlyway(embeddedPostgres)
    private val vedtaksperiodeVentetilstandDao = VedtaksperiodeVentetilstandDao(dataSource)
    private val river = TestRapid().apply {
        VedtaksperiodeVenterRiver(this, dataSource)
        VedtaksperiodeEndretRiver(this, dataSource)
    }

    @AfterAll
    fun tearDown() {
        river.stop()
        dataSource.connection.close()
        embeddedPostgres.close()
    }

    @Test
    fun `vedtaksperiode venter og går videre`(){
        val vedtaksperiodeId = UUID.randomUUID()
        val venterPåVedtaksperiodeId = UUID.randomUUID()
        assertNull(vedtaksperiodeVentetilstandDao.hentOmVenter(vedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId))
        assertNotNull(vedtaksperiodeVentetilstandDao.hentOmVenter(vedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId))
        assertNull(vedtaksperiodeVentetilstandDao.hentOmVenter(vedtaksperiodeId))
    }

    @Test
    fun `Gjentatte like vedtaksperiodeVenter lagres ikke, men så fort noe endres lagres det`() {
        val vedtaksperiodeId = UUID.randomUUID()
        val venterPåVedtaksperiodeId = UUID.randomUUID()
        assertEquals(0, hendelseIderFor(vedtaksperiodeId).size)
        river.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId))
        assertEquals(1, hendelseIderFor(vedtaksperiodeId).size)
        river.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId))
        assertEquals(1, hendelseIderFor(vedtaksperiodeId).size)
        river.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId, "UTBETALING"))
        assertEquals(2, hendelseIderFor(vedtaksperiodeId).size)
        assertEquals("UTBETALING", vedtaksperiodeVentetilstandDao.hentOmVenter(vedtaksperiodeId)!!.venterPå.hva)
        river.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId))
        assertEquals(3, hendelseIderFor(vedtaksperiodeId).size)
        assertNull(vedtaksperiodeVentetilstandDao.hentOmVenter(vedtaksperiodeId))
    }

    @Test
    fun `Begynner å vente på nytt`() {
        val vedtaksperiodeId = UUID.randomUUID()
        val venterPåVedtaksperiodeId = UUID.randomUUID()
        assertNull(vedtaksperiodeVentetilstandDao.hentOmVenter(vedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId))
        assertNotNull(vedtaksperiodeVentetilstandDao.hentOmVenter(vedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId))
        assertNull(vedtaksperiodeVentetilstandDao.hentOmVenter(vedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId))
        assertNotNull(vedtaksperiodeVentetilstandDao.hentOmVenter(vedtaksperiodeId))
        assertEquals(3, hendelseIderFor(vedtaksperiodeId).size)
    }

    @Test
    fun `lagrer riktig ting`() {
        val vedtaksperiodeId = UUID.fromString("00000000-0000-0000-0000-000000000000")
        val venterPåVedtaksperiodeId = UUID.fromString("00000000-0000-0000-0000-000000000001")
        river.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId, "TESTING"))
        val forventet = VedtaksperiodeVenter.opprett(
            vedtaksperiodeId = vedtaksperiodeId,
            fødselsnummer = "11111111111",
            organisasjonsnummer = "123456789",
            ventetSiden = LocalDateTime.parse("2023-03-04T21:34:17"),
            venterTil = LocalDateTime.parse("9999-12-31T23:59:59"),
            venterPå = VenterPå(
                vedtaksperiodeId = venterPåVedtaksperiodeId,
                organisasjonsnummer = "987654321",
                hva = "TESTING",
                hvorfor = "TESTOLINI"
            )
        )
        assertEquals(forventet, vedtaksperiodeVentetilstandDao.hentOmVenter(vedtaksperiodeId))
    }

    @Test
    fun `Ignorerer urelevante vedtaksperiode_endret`() {
        val vedtaksperiodeId = UUID.randomUUID()
        val venterPåVedtaksperiodeId = UUID.randomUUID()
        river.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId))
        assertEquals(0, hendelseIderFor(vedtaksperiodeId).size)
        assertNull(vedtaksperiodeVentetilstandDao.hentOmVenter(vedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId))
        assertEquals(1, hendelseIderFor(vedtaksperiodeId).size)
        assertNotNull(vedtaksperiodeVentetilstandDao.hentOmVenter(vedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId))
        assertEquals(2, hendelseIderFor(vedtaksperiodeId).size)
        assertNull(vedtaksperiodeVentetilstandDao.hentOmVenter(vedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId))
        assertEquals(2, hendelseIderFor(vedtaksperiodeId).size)
    }

    @Language("JSON")
    private fun vedtaksperiodeVenter(vedtaksperiodeId: UUID, venterPåVedtaksperiodeId: UUID, venterPå: String = "GODKJENNING") = """
        {
          "@event_name": "vedtaksperiode_venter",
          "organisasjonsnummer": "123456789",
          "vedtaksperiodeId": "$vedtaksperiodeId",
          "ventetSiden": "2023-03-04T21:34:17.96322",
          "venterTil": "+999999999-12-31T23:59:59.999999999",
          "venterPå": {
            "vedtaksperiodeId": "$venterPåVedtaksperiodeId",
            "organisasjonsnummer": "987654321",
            "venteårsak": {
              "hva": "$venterPå",
              "hvorfor": "TESTOLINI"
            }
          },
          "@id": "${UUID.randomUUID()}",
          "fødselsnummer": "11111111111"
        }
    """

    @Language("JSON")
    private fun vedtaksperiodeEndret(vedtaksperiodeId: UUID) = """
         {
          "@event_name": "vedtaksperiode_endret",
          "organisasjonsnummer": "123456789",
          "vedtaksperiodeId": "$vedtaksperiodeId",
          "gjeldendeTilstand": "AVVENTER_INNTEKTSMELDING",
          "forrigeTilstand": "AVVENTER_INFOTRYGDHISTORIKK",
          "@id": "${UUID.randomUUID()}",
          "fødselsnummer": "11111111111"
        } 
    """

    private fun hendelseIderFor(vedtaksperiodeId: UUID) = sessionOf(dataSource).use { session ->
        session.list(queryOf("SELECT hendelseId FROM vedtaksperiode_ventetilstand WHERE vedtaksperiodeId = :vedtaksperiodeId", mapOf(
            "vedtaksperiodeId" to vedtaksperiodeId
        ))) { it.uuid("hendelseId")}.toSet()
    }
}