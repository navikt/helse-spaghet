package no.nav.helse

import no.nav.helse.rapids_rivers.testsupport.TestRapid
import no.nav.helse.ventetilstand.VedtaksperiodeEndretRiver
import no.nav.helse.ventetilstand.VedtaksperiodeVenterRiver
import no.nav.helse.ventetilstand.VedtaksperiodeVentetilstandDao
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class VedtaksperiodeVentetilstandTest {
    private val embeddedPostgres = embeddedPostgres()
    private val dataSource = setupDataSourceMedFlyway(embeddedPostgres)
    private val vedtaksperiodeDao = VedtaksperiodeVentetilstandDao(dataSource)
    private val river = TestRapid().apply {
        VedtaksperiodeVenterRiver(this, vedtaksperiodeDao)
        VedtaksperiodeEndretRiver(this, vedtaksperiodeDao)
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
        val venterPå = UUID.randomUUID()
        assertNull(vedtaksperiodeDao.hentOmVenter(vedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPå))
        assertNotNull(vedtaksperiodeDao.hentOmVenter(vedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId))
        assertNull(vedtaksperiodeDao.hentOmVenter(vedtaksperiodeId))
    }

    @Language("JSON")
    private fun vedtaksperiodeVenter(vedtaksperiodeId: UUID, venterPå: UUID) = """
        {
          "@event_name": "vedtaksperiode_venter",
          "organisasjonsnummer": "123456789",
          "vedtaksperiodeId": "$vedtaksperiodeId",
          "ventetSiden": "2023-03-04T21:34:17.96322",
          "venterTil": "+999999999-12-31T23:59:59.999999999",
          "venterPå": {
            "vedtaksperiodeId": "$venterPå",
            "organisasjonsnummer": "987654321",
            "venteårsak": {
              "hva": "GODKJENNING",
              "hvorfor": null
            }
          },
          "@id": "${UUID.randomUUID()}",
          "fødselsnummer": "1111111111111"
        }
    """.trimIndent()

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
    """.trimIndent()

}