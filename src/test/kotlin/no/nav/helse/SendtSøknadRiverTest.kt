package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class SendtSøknadRiverTest {
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

    @AfterEach
    fun slettRader() {
        return sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "TRUNCATE TABLE soknad"
            session.run(queryOf(query).asExecute)
        }
    }

    @Test
    fun `lagrer søknadNav`() {
        river.sendTestMessage(sendtSøknadNav())
        assertEquals(1, tellSøknader())
        assertFalse(erSøknadArbeidsgiver())
    }

    @Test
    fun `lagrer søknadArbeidsgiver `() {
        river.sendTestMessage(sendtSøknadArbeidsgiver())
        assertEquals(1, tellSøknader())
        assertTrue(erSøknadArbeidsgiver())
    }

    private fun tellSøknader(): Int {
        return sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT COUNT(*) FROM soknad"
            requireNotNull(
                session.run(queryOf(query).map { row -> row.int(1) }.asSingle)
            )
        }
    }

    private fun erSøknadArbeidsgiver(): Boolean {
        return sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT kort_soknad FROM soknad"
            requireNotNull(
                session.run(queryOf(query).map { row -> row.boolean(1) }.asSingle)
            )
        }
    }

    private fun sendtSøknadArbeidsgiver() = """
        {
        "id": "c06e3ec2-ba0d-3dca-8db1-5b3200c63745",
        "@id": "162fc482-d871-4036-94af-a79112d82abe",
        "@event_name": "sendt_søknad_arbeidsgiver"
        }
    """

    private fun sendtSøknadNav() = """
        {
        "id": "c06e3ec2-ba0d-3dca-8db1-5b3200c63745",
        "@id": "162fc482-d871-4036-94af-a79112d82abe",
        "@event_name": "sendt_søknad_nav"
        }
    """
}