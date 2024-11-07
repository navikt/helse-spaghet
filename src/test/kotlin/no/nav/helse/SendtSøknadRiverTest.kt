package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.E2eTestApp.Companion.e2eTest
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

internal class SendtSøknadRiverTest {
    @Test
    fun `lagrer søknadNav`() = e2eTest {
        rapid.sendTestMessage(sendtSøknadNav())
        assertEquals(1, tellSøknader())
        assertEquals("sendt_søknad_nav", eventName())
    }

    @Test
    fun `lagrer søknadArbeidsgiver`() = e2eTest {
        rapid.sendTestMessage(sendtSøknadArbeidsgiver())
        assertEquals(1, tellSøknader())
        assertEquals("sendt_søknad_arbeidsgiver", eventName())
    }

    @Test
    fun `lagrer søknadArbeidsledig`() = e2eTest {
        rapid.sendTestMessage(sendtSøknadArbeidsledig())
        assertEquals(1, tellSøknader())
        assertEquals("sendt_søknad_arbeidsledig", eventName())
    }

    @Test
    fun `lagrer søknadFrilanser`() = e2eTest {
        rapid.sendTestMessage(sendtSøknadFrilanser())
        assertEquals(1, tellSøknader())
        assertEquals("sendt_søknad_frilanser", eventName())
    }

    @Test
    fun `lagrer søknadSelvstendigNæringsdrivende`() = e2eTest {
        rapid.sendTestMessage(sendtSøknadSelvstendigNæringsdrivende())
        assertEquals(1, tellSøknader())
        assertEquals("sendt_søknad_selvstendig", eventName())
    }

    private fun E2eTestApp.tellSøknader(): Int {
        return sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT COUNT(*) FROM soknad"
            requireNotNull(
                session.run(queryOf(query).map { row -> row.int(1) }.asSingle)
            )
        }
    }

    private fun E2eTestApp.eventName(): String {
        return sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT event FROM soknad"
            requireNotNull(
                session.run(queryOf(query).map { row -> row.string(1) }.asSingle)
            )
        }
    }

    private fun sendtSøknadArbeidsgiver() = """
        {
        "id": "c06e3ec2-ba0d-3dca-8db1-5b3200c63745",
        "@id": "162fc482-d871-4036-94af-a79112d82abe",
        "@event_name": "sendt_søknad_arbeidsgiver", 
        "type": "ARBEIDSTAKERE",
        "arbeidssituasjon": "ARBEIDSTAKER"
        }
    """

    private fun sendtSøknadNav() = """
        {
        "id": "c06e3ec2-ba0d-3dca-8db1-5b3200c63745",
        "@id": "162fc482-d871-4036-94af-a79112d82abe",
        "@event_name": "sendt_søknad_nav", 
        "type": "ARBEIDSTAKERE",
        "arbeidssituasjon": "ARBEIDSTAKER"
        }
    """

    private fun sendtSøknadArbeidsledig() = """
        {
        "id": "c06e3ec2-ba0d-3dca-8db1-5b3200c63745",
        "@id": "162fc482-d871-4036-94af-a79112d82abe",
        "@event_name": "sendt_søknad_arbeidsledig", 
        "type": "ARBEIDSLEDIG",
        "arbeidssituasjon": "ARBEIDSLEDIG"
        }
    """

    private fun sendtSøknadFrilanser() = """
        {
        "id": "c06e3ec2-ba0d-3dca-8db1-5b3200c63745",
        "@id": "162fc482-d871-4036-94af-a79112d82abe",
        "@event_name": "sendt_søknad_frilanser", 
        "type": "SELVSTENDIGE_OG_FRILANSERE",
        "arbeidssituasjon": "FRILANSER"
        }
    """

    private fun sendtSøknadSelvstendigNæringsdrivende() = """
        {
        "id": "c06e3ec2-ba0d-3dca-8db1-5b3200c63745",
        "@id": "162fc482-d871-4036-94af-a79112d82abe",
        "@event_name": "sendt_søknad_selvstendig", 
        "type": "SELVSTENDIGE_OG_FRILANSERE",
        "arbeidssituasjon": "SELVSTENDIG_NARINGSDRIVENDE"
        }
    """
}