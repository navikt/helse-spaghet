package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.E2eTestApp.Companion.e2eTest
import no.nav.helse.TestData.lagtPåVent
import no.nav.helse.TestData.lagtPåVentFlereÅrsakerUtenNotat
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class LagtPåVentE2ETest {

    @Test
    fun `kan lese inn lagt_på_vent`() {
        e2eTest {
            lagtPåVent.sendTilRapid(true)
            assertEquals(1, tellLagtPåVent())
            assertEquals(1, tellLagtPåVentÅrsaker())
        }
    }

    @Test
    fun `kan lese inn lagt_på_vent med flere årsaker uten notat`() {
        e2eTest {
            lagtPåVentFlereÅrsakerUtenNotat.sendTilRapid(false)
            assertEquals(1, tellLagtPåVent())
            assertEquals(2, tellLagtPåVentÅrsaker())
        }
    }

    private fun E2eTestApp.tellLagtPåVent(): Int {
        return sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT COUNT(*) FROM lagt_paa_vent"
            requireNotNull(
                session.run(queryOf(query).map { row -> row.int(1) }.asSingle)
            )
        }
    }

    private fun E2eTestApp.tellLagtPåVentÅrsaker(): Int {
        return sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT COUNT(*) FROM lagt_paa_vent_arsak"
            requireNotNull(
                session.run(queryOf(query).map { row -> row.int(1) }.asSingle)
            )
        }
    }
}