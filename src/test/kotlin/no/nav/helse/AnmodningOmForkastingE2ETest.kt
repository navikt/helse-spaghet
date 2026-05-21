package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.E2eTestApp.Companion.e2eTest
import no.nav.helse.TestData.AnmodningOmForkastingMelding
import no.nav.helse.TestData.anmodningMedAvsender
import no.nav.helse.TestData.anmodningMedSaksbehandlerIdent
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class AnmodningOmForkastingE2ETest {

    @Test
    fun `lagrer anmodning om forkasting med kun saksbehandlerIdent`() {
        e2eTest {
            anmodningMedSaksbehandlerIdent.sendTilRapid()
            assertEquals(1, tellAnmodninger())
            assertEquals("NB111111", hentAvsender())
        }
    }

    @Test
    fun `lagrer anmodning om forkasting med kun avsender NavIdent`() {
        e2eTest {
            anmodningMedAvsender.sendTilRapid()
            assertEquals(1, tellAnmodninger())
            assertEquals("NB222222", hentAvsender())
        }
    }

    @Test
    fun `saksbehandlerIdent vinner over avsender NavIdent når begge er tilstede`() {
        e2eTest {
            AnmodningOmForkastingMelding().beggeAvsendere(saksbehandler = "NB111111", avsender = "NB222222").sendTilRapid()
            assertEquals(1, tellAnmodninger())
            assertEquals("NB111111", hentAvsender())
        }
    }

    @Test
    fun `ignorerer melding uten verken saksbehandlerIdent eller avsender`() {
        e2eTest {
            AnmodningOmForkastingMelding().ingenAvsender().sendTilRapid()
            assertEquals(0, tellAnmodninger())
        }
    }

    @Test
    fun `lagrer to separate anmodninger`() {
        e2eTest {
            anmodningMedSaksbehandlerIdent.sendTilRapid()
            anmodningMedAvsender.sendTilRapid()
            assertEquals(2, tellAnmodninger())
        }
    }

    @Test
    fun `lagrer kommentar når den er tilstede`() {
        e2eTest {
            anmodningMedSaksbehandlerIdent.copy(kommentar = "Denne perioden skal forkastes").sendTilRapid()
            assertEquals("Denne perioden skal forkastes", hentKommentar())
        }
    }

    @Test
    fun `kommentar er null når den ikke er tilstede`() {
        e2eTest {
            anmodningMedSaksbehandlerIdent.sendTilRapid()
            assertEquals(null, hentKommentar())
        }
    }

    private fun E2eTestApp.tellAnmodninger(): Int =
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT COUNT(*) FROM anmodning_om_forkasting"
            requireNotNull(session.run(queryOf(query).map { row -> row.int(1) }.asSingle))
        }

    private fun E2eTestApp.hentAvsender(): String? =
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT avsender FROM anmodning_om_forkasting LIMIT 1"
            session.run(queryOf(query).map { row -> row.string("avsender") }.asSingle)
        }

    private fun E2eTestApp.hentKommentar(): String? =
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT kommentar FROM anmodning_om_forkasting LIMIT 1"
            session.run(queryOf(query).map { row -> row.stringOrNull("kommentar") }.asSingle)
        }
}
