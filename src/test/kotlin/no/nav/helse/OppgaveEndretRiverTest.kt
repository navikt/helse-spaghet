package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.E2eTestApp.Companion.e2eTest
import no.nav.helse.TestData.nyOppgave
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class OppgaveEndretRiverTest {

    @Test
    fun `lagre oppgave i databasen`() {
        e2eTest {
            val oppgave = nyOppgave
            oppgave.sendTilRapid()
            assertEquals(1, oppgaverForId(oppgave.id))
        }
    }

    @Test
    fun `lagrer ikke oppgave dobbelt i databasen`() {
        e2eTest {
            val oppgave = nyOppgave
            oppgave.sendTilRapid()
            oppgave.sendTilRapid()
            assertEquals(1, oppgaverForId(oppgave.id))
        }
    }

    @Test
    fun `lagrer oppgaveendring i databasen`() {
        e2eTest {
            val oppgave = nyOppgave
            oppgave.sendTilRapid()
            assertEquals(1, oppgaveendringerForId(oppgave.id))
        }
    }

    @Test
    fun `lagrer én oppgaveendring per oppgavemelding`() {
        e2eTest {
            val oppgave = nyOppgave
            oppgave.sendTilRapid()
            oppgave
                .egenskaper("SØKNAD", "RISK_QA")
                .sendTilRapid()
            assertEquals(2, oppgaveendringerForId(oppgave.id))
        }
    }

    private fun E2eTestApp.oppgaverForId(id: Long) =
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT count(1) FROM oppgave WHERE id = ?"
            session.run(
                queryOf(query, id)
                    .map { it.int(1) }
                    .asSingle
            ) ?: 0
        }

    private fun E2eTestApp.oppgaveendringerForId(id: Long) =
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT count(1) FROM oppgave_endret WHERE oppgave_ref = ?"
            session.run(
                queryOf(query, id)
                    .map { it.int(1) }
                    .asSingle
            ) ?: 0
        }
}