package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import no.nav.helse.rapids_rivers.testsupport.TestRapid
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
    fun `lagrer godkjenninger for gammel rapport`() {
        val fødselsnummer = "782346238"
        val vedtaksperiodeId = UUID.randomUUID()
        val id = UUID.randomUUID()
        river.sendTestMessage(behovNyttFormat(fødselsnummer, vedtaksperiodeId, "FORLENGELSE", id))
        river.sendTestMessage(løsningNyttFormat(fødselsnummer, vedtaksperiodeId, "FORLENGELSE", id))

        assertEquals(listOf(vedtaksperiodeId.toString()), finnGodkjenninger(fødselsnummer))
    }

    @Test
    fun `finner aktiviteter for hendelse`() {
        val vedtaksperiodeId = UUID.randomUUID()
        val id = UUID.randomUUID()
        river.sendTestMessage(vedtaksperiodeEndret(hendelseId = id, vedtaksperiodeId = vedtaksperiodeId))

        assertEquals(listOf(
                "Behandler simulering",
                "Simulering kom frem til et annet totalbeløp. Kontroller beløpet til utbetaling"
        ), finnAktiviteter(id))
    }

    @Test
    fun `finner vedtaksperiode_endret for hendelse`() {
        val vedtaksperiodeId = UUID.randomUUID()
        val id = UUID.randomUUID()
        river.sendTestMessage(vedtaksperiodeEndret(hendelseId = id, vedtaksperiodeId = vedtaksperiodeId))

        assertEquals("AVVENTER_GODKJENNING", finnVedtaksperiodeTilstandsendring(id))
    }

    @Test
    fun `defaulter til ident om saksbehandler oid ikke eksisterer`() {
        val fødselsnummer = "83497290"
        val vedtaksperiodeId = UUID.randomUUID()
        val id = UUID.randomUUID()
        val saksbehandlerIdent = "Z111111"
        river.sendTestMessage(behovNyttFormat(fødselsnummer, vedtaksperiodeId, "FORLENGELSE", id))
        river.sendTestMessage(løsningNyttFormat(fødselsnummer, vedtaksperiodeId, "FORLENGELSE", id, saksbehandlerIdent = saksbehandlerIdent))

        assertEquals(saksbehandlerIdent, finnSaksbehandlerIdentitet(id))
    }

    @Test
    fun `defaulter til spesialist oid om det er en automatisk behandling`() {
        val fødselsnummer = "83497290"
        val vedtaksperiodeId = UUID.randomUUID()
        val id = UUID.randomUUID()
        val saksbehandlerIdent = "Automatisk behandlet"
        river.sendTestMessage(behovNyttFormat(fødselsnummer, vedtaksperiodeId, "FORLENGELSE", id))
        river.sendTestMessage(løsningNyttFormat(fødselsnummer, vedtaksperiodeId, "FORLENGELSE", id, automatiskBehandlet = true, saksbehandlerIdent = saksbehandlerIdent))

        assertEquals(VedtaksperiodeBehandletRiver.SPESIALIST_OID, finnSaksbehandlerIdentitet(id))
    }

    @Test
    fun `godkjenningsbehov blir lest fra rapid`() {
        val fødselsnummer = "1243356"
        val vedtaksperiodeId = UUID.randomUUID()
        val id = UUID.randomUUID()
        river.sendTestMessage(behovNyttFormat(fødselsnummer, vedtaksperiodeId, "FORLENGELSE", id))
        river.sendTestMessage(løsningNyttFormat(fødselsnummer, vedtaksperiodeId, "FORLENGELSE", id))

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

        assertEquals(listOf(id.toString()), finnGodkjenningsbehovLøsning(id))
        assertEquals(listOf(id.toString()), finnGodkjenningsbehovLøsningBegrunnelse(id))
    }

    @Test
    fun `persisterer periodetype for nye godkjenninger`() {
        val id = UUID.randomUUID()
        river.sendTestMessage(behovNyttFormat("8756876", UUID.randomUUID(), "FORLENGELSE", id))

        assertEquals("FORLENGELSE", finnPeriodetype(id))
    }

    @Test
    fun `persisterer periodetype for gamle godkjenninger`() {
        val id = UUID.randomUUID()
        river.sendTestMessage(gammelBehov("8756876", UUID.randomUUID(), "FORLENGELSE", id))

        assertEquals("FORLENGELSE", finnPeriodetype(id))
    }

    private fun finnGodkjenninger(fødselsnummer: String) = using(sessionOf(dataSource)) { session ->
        session.run(queryOf("SELECT * FROM godkjenning WHERE fodselsnummer=?;", fødselsnummer)
                .map { it.string("vedtaksperiode_id") }
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

    private fun finnAktiviteter(id: UUID) = sessionOf(dataSource).use { session ->
        session.run(queryOf("SELECT * FROM vedtaksperiode_aktivitet WHERE hendelse_id=?;", id)
                .map { it.string("melding") }
                .asList)
    }

    private fun finnVedtaksperiodeTilstandsendring(id: UUID) = sessionOf(dataSource).use { session ->
        session.run(queryOf("SELECT * FROM vedtaksperiode_tilstandsendring WHERE hendelse_id=?;", id)
                .map { it.string("tilstand_til") }
                .asSingle)
    }

    private fun finnPeriodetype(id: UUID) = sessionOf(dataSource).use { session ->
        session.run(queryOf("SELECT * FROM godkjenningsbehov WHERE id=?;", id)
                .map { it.stringOrNull("periodetype") }
                .asSingle)
    }

    private fun finnSaksbehandlerIdentitet(id: UUID) = sessionOf(dataSource).use { session ->
        session.run(queryOf("SELECT * FROM godkjenningsbehov_losning WHERE id=?;", id)
                .map { it.string("godkjent_av") }
                .asSingle)
    }
}
