package no.nav.helse

import no.nav.helse.E2eTestApp.Companion.e2eTest
import no.nav.helse.TestData.annullering
import no.nav.helse.TestData.begrunnelse
import no.nav.helse.TestData.fagsystemId
import no.nav.helse.TestData.kommentar
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

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
            annullering
                .fagsystemId("banan")
                .sendTilRapid()
            val lagrede = dataSource.annulleringer()
            assertEquals(2, lagrede.size)
            assertEquals(setOf(annullering, annullering.fagsystemId("banan")), lagrede.toSet())
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
}