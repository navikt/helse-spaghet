package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SpaghetE2ETest {
    private val embeddedPostgres = embeddedPostgres()
    private val dataSource = setupDataSourceMedFlyway(embeddedPostgres)
    private val river = TestRapid()
        .setupRiver(dataSource)

    @AfterAll
    fun tearDown() {
        river.stop()
        dataSource.connection.close()
        embeddedPostgres.close()
    }

    @Test
    fun `godkjenningsbehov blir lest fra rapid`() {
        val fødselsnummer = "1243356"
        val vedtaksperiodeId = UUID.randomUUID()
        val id = UUID.randomUUID()
        river.sendTestMessage(nyBehov(fødselsnummer, vedtaksperiodeId, "FORLENGELSE", id))
        river.sendTestMessage(nyLøsning(fødselsnummer, vedtaksperiodeId, "FORLENGELSE", id))

        assertEquals(listOf(vedtaksperiodeId.toString()), finnGodkjenninger(fødselsnummer))
        assertEquals(listOf(id.toString()), finnGodkjenningsbehovLøsning(id))
        assertEquals(listOf(id.toString()), finnGodkjenningsbehovLøsningBegrunnelse(id))
    }

    @Test
    fun `gamle godkjenningsbehov uten løsning blir lest fra rapid`() {
        val fødselsnummer = "2243356"
        val vedtaksperiodeId = UUID.randomUUID()
        val id = UUID.randomUUID()
        river.sendTestMessage(gammelBehov(fødselsnummer, vedtaksperiodeId, "FORLENGELSE", id))
        river.sendTestMessage(gammelLøsning(fødselsnummer, vedtaksperiodeId, "FORLENGELSE", id))

        // assertEquals(1, finnWarnings(vedtaksperiodeId).size)
        assertEquals(1, finnGodkjenningsbehovLøsning(id).size)
    }

    private fun finnGodkjenninger(fødselsnummer: String) = using(sessionOf(dataSource)) { session ->
        session.run(queryOf("SELECT * FROM godkjenning WHERE fodselsnummer=?;", fødselsnummer)
            .map { it.string("vedtaksperiode_id") }
            .asList)
    }

    private fun finnWarnings(vedtaksperiode: UUID) = using(sessionOf(dataSource)) { session ->
        session.run(queryOf("SELECT * FROM godkjenningsbehov_warning WHERE vedtaksperiode_id=?;", vedtaksperiode)
            .map { it.string("melding") }
            .asList)
    }

    private fun finnGodkjenningsbehovLøsning(id: UUID) = using(sessionOf(dataSource)) { session ->
        session.run(queryOf("SELECT * FROM godkjenningsbehov_losning WHERE id=?;", id)
            .map { it.string("id") }
            .asList)
    }

    private fun finnGodkjenningsbehovLøsningBegrunnelse(id: UUID) = using(sessionOf(dataSource)) { session ->
        session.run(queryOf("SELECT * FROM godkjenningsbehov_losning_begrunnelse WHERE id=?;", id)
            .map { it.string("id") }
            .asList)
    }

}
