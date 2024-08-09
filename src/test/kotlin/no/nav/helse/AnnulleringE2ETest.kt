package no.nav.helse

import no.nav.helse.E2eTestApp.Companion.e2eTest
import no.nav.helse.TestData.annullering
import no.nav.helse.TestData.begrunnelse
import no.nav.helse.TestData.kommentar
import no.nav.helse.TestData.vedtaksperiodeId
import no.nav.helse.TestData.årsak
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.util.*

class AnnulleringE2ETest {
    @Test
    fun `Happy path`() {
        e2eTest {
            annullering.sendTilRapid()
            val lagrede = dataSource.annulleringer()
            assertEquals(1, lagrede.size)
            assertEquals(annullering, lagrede[0])
        }
    }

    @Test
    fun `Dedup på fagsystemId`() {
        e2eTest {
            annullering.sendTilRapid()
            annullering.sendTilRapid()
            val lagrede = dataSource.annulleringer()
            assertEquals(1, lagrede.size)
            assertEquals(annullering, lagrede[0])
        }
    }

    @Test
    fun `To events`() {
        e2eTest {
            annullering.sendTilRapid()
            val vedtaksperiodeId = UUID.randomUUID()
            annullering
                .vedtaksperiodeId(vedtaksperiodeId)
                .sendTilRapid()
            val lagrede = dataSource.annulleringer()
            assertEquals(2, lagrede.size)
            assertEquals(setOf(annullering, annullering.vedtaksperiodeId(vedtaksperiodeId)), lagrede.toSet())
        }
    }

    @Test
    fun `Med kommentar`() {
        e2eTest {
            annullering
                .kommentar("Kremfjes")
                .sendTilRapid()
            assertEquals("Kremfjes", dataSource.annulleringer()[0].kommentar)
        }
    }

    @Test
    fun `Med årsak`() {
        e2eTest {
            annullering
                .begrunnelse("Kremfjes")
                .årsak(AnnulleringArsak(key = "key01", arsak = "Ferie"))
                .sendTilRapid()
            assertEquals(listOf("Ferie"), dataSource.annulleringer()[0].begrunnelser)
            assertEquals(listOf(AnnulleringArsak(key = "key01", arsak = "Ferie")), dataSource.annulleringer()[0].arsaker)
        }
    }

    @Test
    fun `Fler begrunnelse`() {
        e2eTest {
            annullering
                .begrunnelse("Kremfjes")
                .begrunnelse("Hammer av gøy")
                .sendTilRapid()
            assertEquals(listOf("Kremfjes", "Hammer av gøy"), dataSource.annulleringer()[0].begrunnelser)
        }
    }

    @Test
    fun `En begrunnelse`() {
        e2eTest {
            annullering
                .kommentar("Kremfjes")
                .sendTilRapid()
            assertEquals("Kremfjes", dataSource.annulleringer()[0].kommentar)
        }
    }

    @Test
    fun `Folk som skriver identer i oid-feltet skal ikke ta ned spaghet`() {
        val feilMelding = """{
            "@event_name": "annullering",
            "saksbehandler": {"oid": "X000000"},
            "vedtaksperiodeId": "0344ff99-b92b-4c35-ae2e-75964666099d",
            "begrunnelser": ["because"],
            "@opprettet": "${LocalDateTime.now()}"
         }""".trimMargin()
        e2eTest {
            rapid.sendTestMessage(feilMelding)
            assertEquals(0, dataSource.annulleringer().size)
        }

    }
}
