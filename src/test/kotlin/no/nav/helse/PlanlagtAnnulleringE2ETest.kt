package no.nav.helse

import no.nav.helse.E2eTestApp.Companion.e2eTest
import no.nav.helse.TestData.annullering
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.util.*

class PlanlagtAnnulleringE2ETest {

    @Test
    fun `Utløsende vedtaksperiode lagres som berørt vedtaksperiode`() {
        e2eTest {
            val utløsendeVedtaksperiodeId = UUID.randomUUID()
            annullering.copy(vedtaksperiodeId = utløsendeVedtaksperiodeId).sendTilRapid()

            rapid.sendTestMessage(planlagtAnnullering(vedtaksperioder = listOf(utløsendeVedtaksperiodeId)))

            val lagrede = dataSource.annulleringBerorteVedtaksperioder()
            assertEquals(1, lagrede.size)
            assertEquals(utløsendeVedtaksperiodeId, lagrede[0].vedtaksperiodeId)
            assertEquals(utløsendeVedtaksperiodeId, lagrede[0].utløsendeVedtaksperiodeId)
        }
    }

    @Test
    fun `Lagrer alle berørte vedtaksperioder med utløsende vedtaksperiode id`() {
        e2eTest {
            val utløsendeVedtaksperiodeId = UUID.randomUUID()
            val berørtVedtaksperiodeId = UUID.randomUUID()
            annullering.copy(vedtaksperiodeId = utløsendeVedtaksperiodeId).sendTilRapid()

            rapid.sendTestMessage(planlagtAnnullering(vedtaksperioder = listOf(utløsendeVedtaksperiodeId, berørtVedtaksperiodeId)))

            val lagrede = dataSource.annulleringBerorteVedtaksperioder().sortedBy { it.vedtaksperiodeId }
            assertEquals(2, lagrede.size)
            assertEquals(utløsendeVedtaksperiodeId, lagrede[0].utløsendeVedtaksperiodeId)
            assertEquals(utløsendeVedtaksperiodeId, lagrede[1].utløsendeVedtaksperiodeId)
            assertEquals(listOf(berørtVedtaksperiodeId, utløsendeVedtaksperiodeId).sorted(), lagrede.map { it.vedtaksperiodeId })
        }
    }

    @Test
    fun `Ignorerer planlagt annullering uten utløsende vedtaksperiode i annullering`() {
        e2eTest {
            val ids = listOf(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID())

            rapid.sendTestMessage(planlagtAnnullering(vedtaksperioder = ids))

            assertEquals(0, dataSource.annulleringBerorteVedtaksperioder().size)
        }
    }

    @Test
    fun `Dedup - samme vedtaksperioder lagres kun én gang`() {
        e2eTest {
            val utløsendeVedtaksperiodeId = UUID.randomUUID()
            val berørtVedtaksperiodeId = UUID.randomUUID()
            annullering.copy(vedtaksperiodeId = utløsendeVedtaksperiodeId).sendTilRapid()

            rapid.sendTestMessage(planlagtAnnullering(vedtaksperioder = listOf(utløsendeVedtaksperiodeId, berørtVedtaksperiodeId)))
            rapid.sendTestMessage(planlagtAnnullering(vedtaksperioder = listOf(utløsendeVedtaksperiodeId, berørtVedtaksperiodeId)))

            assertEquals(2, dataSource.annulleringBerorteVedtaksperioder().size)
        }
    }

    @Test
    fun `Planlagt annullering på et selvstendig sykefravær`() {
        e2eTest {
            val utløsendeVedtaksperiodeId = UUID.randomUUID()
            val berørtVedtaksperiodeId = UUID.randomUUID()
            annullering.copy(vedtaksperiodeId = utløsendeVedtaksperiodeId).sendTilRapid()

            rapid.sendTestMessage(planlagtAnnulleringSelvstendig(vedtaksperioder = listOf(utløsendeVedtaksperiodeId, berørtVedtaksperiodeId)))

            val lagrede = dataSource.annulleringBerorteVedtaksperioder().sortedBy { it.vedtaksperiodeId }
            assertEquals(2, lagrede.size)
            assertEquals(utløsendeVedtaksperiodeId, lagrede[0].utløsendeVedtaksperiodeId)
            assertEquals(utløsendeVedtaksperiodeId, lagrede[1].utløsendeVedtaksperiodeId)
            assertEquals(listOf(berørtVedtaksperiodeId, utløsendeVedtaksperiodeId).sorted(), lagrede.map { it.vedtaksperiodeId })
            assertEquals("SELVSTENDIG", lagrede[0].yrkesaktivitetstype)
            assertEquals("SELVSTENDIG", lagrede[0].organisasjonsnummer)
        }
    }

    private fun planlagtAnnullering(
        vedtaksperioder: List<UUID>,
        fom: LocalDate = LocalDate.of(2025, 11, 14),
        tom: LocalDate = LocalDate.of(2025, 12, 12),
        organisasjonsnummer: String = "974016370",
        yrkesaktivitetstype: String = "ARBEIDSTAKER",
    ) = """
            {
              "@event_name": "planlagt_annullering",
              "yrkesaktivitetstype": "$yrkesaktivitetstype",
              "organisasjonsnummer": "$organisasjonsnummer",
              "vedtaksperioder": [${vedtaksperioder.joinToString {""""$it""""}}],
              "fom": "$fom",
              "tom": "$tom",
              "@id": "${UUID.randomUUID()}",
              "@opprettet": "2026-05-21T10:32:27.796819"
            }
            """.trimIndent()

    private fun planlagtAnnulleringSelvstendig(
        vedtaksperioder: List<UUID>,
        fom: LocalDate = LocalDate.of(2025, 11, 14),
        tom: LocalDate = LocalDate.of(2025, 12, 12),
        yrkesaktivitetstype: String = "SELVSTENDIG",
    ) = """
            {
              "@event_name": "planlagt_annullering",
              "yrkesaktivitetstype": "$yrkesaktivitetstype",
              "vedtaksperioder": [${vedtaksperioder.joinToString {""""$it""""}}],
              "fom": "$fom",
              "tom": "$tom",
              "@id": "${UUID.randomUUID()}",
              "@opprettet": "2026-05-21T10:32:27.796819"
            }
            """.trimIndent()
}

