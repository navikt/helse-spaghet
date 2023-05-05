package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SpaghetE2ETest {
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
    fun `finner vedtaksperiode_endret for hendelse`() {
        val vedtaksperiodeId = UUID.randomUUID()
        val id = UUID.randomUUID()
        river.sendTestMessage(vedtaksperiodeEndret(id = id, vedtaksperiodeId = vedtaksperiodeId))

        assertEquals(listOf("AVVENTER_GODKJENNING"), finnVedtaksperiodeTilstandsendring(id))
    }

    @Test
    fun `finner ikke vedtaksperiode_endret for hendelse som forblir i samme tilstand`() {
        val vedtaksperiodeId = UUID.randomUUID()
        val id = UUID.randomUUID()
        river.sendTestMessage(
            vedtaksperiodeEndret(
                id = id,
                vedtaksperiodeId = vedtaksperiodeId,
                tilstandFra = "AVSLUTTET_UTEN_UTBETALING",
                tilstandTil = "AVSLUTTET_UTEN_UTBETALING"
            )
        )

        assertEquals(emptyList<String>(), finnVedtaksperiodeTilstandsendring(id))
    }

    @Test
    fun `defaulter til ident om saksbehandler oid ikke eksisterer`() {
        val fødselsnummer = "83497290"
        val vedtaksperiodeId = UUID.randomUUID()
        val id = UUID.randomUUID()
        val saksbehandlerIdent = "Z111111"
        river.sendTestMessage(behovNyttFormat(fødselsnummer, vedtaksperiodeId, "FORLENGELSE", id))
        river.sendTestMessage(
            løsningNyttFormat(
                fødselsnummer,
                vedtaksperiodeId,
                "FORLENGELSE",
                id,
                saksbehandlerIdent = saksbehandlerIdent
            )
        )

        assertEquals(saksbehandlerIdent, finnSaksbehandlerIdentitet(id))
    }

    @Test
    fun `defaulter til spesialist oid om det er en automatisk behandling`() {
        val fødselsnummer = "83497290"
        val vedtaksperiodeId = UUID.randomUUID()
        val id = UUID.randomUUID()
        val saksbehandlerIdent = "Automatisk behandlet"
        river.sendTestMessage(behovNyttFormat(fødselsnummer, vedtaksperiodeId, "FORLENGELSE", id))
        river.sendTestMessage(
            løsningNyttFormat(
                fødselsnummer,
                vedtaksperiodeId,
                "FORLENGELSE",
                id,
                automatiskBehandlet = true,
                saksbehandlerIdent = saksbehandlerIdent
            )
        )

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
    fun `godkjenningsbehov blir lest fra rapid uten refusjonstype`() {
        val fødselsnummer = "1243356"
        val vedtaksperiodeId = UUID.randomUUID()
        val id = UUID.randomUUID()
        river.sendTestMessage(behovNyttFormat(fødselsnummer, vedtaksperiodeId, "FORLENGELSE", id))
        river.sendTestMessage(løsningNyttFormat(fødselsnummer, vedtaksperiodeId, "FORLENGELSE", id, refusjonstype = null))

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
    fun `persisterer inntektskilde`() {
        val id1 = UUID.randomUUID()
        val id2 = UUID.randomUUID()

        val fødselsnummer = "83497290"

        river.sendTestMessage(behovNyttFormat(fødselsnummer, UUID.randomUUID(), "FORLENGELSE", id1, "EN_ARBEIDSGIVER"))
        river.sendTestMessage(
            behovNyttFormat(
                fødselsnummer,
                UUID.randomUUID(),
                "FORLENGELSE",
                id2,
                "FLERE_ARBEIDSGIVERE"
            )
        )

        assertEquals("EN_ARBEIDSGIVER", finnInntektskilde(id1))
        assertEquals("FLERE_ARBEIDSGIVERE", finnInntektskilde(id2))
    }

    @Test
    fun `lagrer kun behov en gang`() {
        val eventId = UUID.randomUUID()
        val vedtaksperiodeId = UUID.randomUUID()
        val behov = behovNyttFormat("colemak", vedtaksperiodeId, "FORLENGELSE", eventId)

        river.sendTestMessage(behov)
        river.sendTestMessage(behov)

        assertEquals(1, finnGodkjenningsbehov(vedtaksperiodeId).size)
    }

    @Test
    fun `lagrer kun løsning en gang`() {
        val eventId = UUID.randomUUID()
        val vedtaksperiodeId = UUID.randomUUID()

        river.sendTestMessage(behovNyttFormat("colemak", vedtaksperiodeId, "FORLENGELSE", eventId))

        val løsning = løsningNyttFormat("colemak", vedtaksperiodeId, "FORLENGELSE", eventId)
        river.sendTestMessage(løsning)
        river.sendTestMessage(løsning)

        assertEquals(1, finnGodkjenningsbehovLøsning(eventId).size)
        assertEquals(1, finnGodkjenningsbehovLøsningBegrunnelse(eventId).size)
    }

    @Test
    fun `lagrer kun tilstandsendring en gang`() {
        val vedtaksperiodeId = UUID.randomUUID()
        val id = UUID.randomUUID()
        river.sendTestMessage(vedtaksperiodeEndret(id = id, vedtaksperiodeId = vedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeEndret(id = id, vedtaksperiodeId = vedtaksperiodeId))

        assertEquals(1, finnVedtaksperiodeTilstandsendring(id).size)
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

    private fun finnVedtaksperiodeTilstandsendring(id: UUID) = sessionOf(dataSource).use { session ->
        session.run(queryOf("SELECT * FROM vedtaksperiode_tilstandsendring WHERE id=?;", id)
            .map { it.string("tilstand_til") }
            .asList)
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

    private fun finnInntektskilde(id: UUID) = sessionOf(dataSource).use { session ->
        session.run(queryOf("SELECT * FROM godkjenningsbehov WHERE id=?;", id)
            .map { it.stringOrNull("inntektskilde") }
            .asSingle)
    }

    private fun finnGodkjenningsbehov(vedtaksperiodeId: UUID) = sessionOf(dataSource).use { session ->
        session.run(queryOf("SELECT * FROM godkjenningsbehov WHERE vedtaksperiode_id=?;", vedtaksperiodeId)
            .map { it.string("id") }
            .asList)

    }
}
