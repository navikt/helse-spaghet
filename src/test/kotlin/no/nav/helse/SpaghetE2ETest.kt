package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.E2eTestApp.Companion.e2eTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.*

class SpaghetE2ETest {
    @Test
    fun `lagrer godkjenninger for gammel rapport`() =
        e2eTest {
            val fødselsnummer = "782346238"
            val vedtaksperiodeId = UUID.randomUUID()
            val id = UUID.randomUUID()
            rapid.sendTestMessage(behovNyttFormat(fødselsnummer, vedtaksperiodeId, "FORLENGELSE", id))
            rapid.sendTestMessage(løsningNyttFormat(fødselsnummer, vedtaksperiodeId, "FORLENGELSE", id))

            assertEquals(listOf(vedtaksperiodeId.toString()), finnGodkjenninger(fødselsnummer))
        }

    @Test
    fun `finner vedtaksperiode_endret for hendelse`() =
        e2eTest {
            val vedtaksperiodeId = UUID.randomUUID()
            val id = UUID.randomUUID()
            rapid.sendTestMessage(vedtaksperiodeEndret(id = id, vedtaksperiodeId = vedtaksperiodeId))

            assertEquals(listOf("AVVENTER_GODKJENNING"), finnVedtaksperiodeTilstandsendring(id))
        }

    @Test
    fun `finner ikke vedtaksperiode_endret for hendelse som forblir i samme tilstand`() =
        e2eTest {
            val vedtaksperiodeId = UUID.randomUUID()
            val id = UUID.randomUUID()
            rapid.sendTestMessage(
                vedtaksperiodeEndret(
                    id = id,
                    vedtaksperiodeId = vedtaksperiodeId,
                    tilstandFra = "AVSLUTTET_UTEN_UTBETALING",
                    tilstandTil = "AVSLUTTET_UTEN_UTBETALING",
                ),
            )

            assertEquals(emptyList<String>(), finnVedtaksperiodeTilstandsendring(id))
        }

    @Test
    fun `defaulter til ident om saksbehandler oid ikke eksisterer`() =
        e2eTest {
            val fødselsnummer = "83497290"
            val vedtaksperiodeId = UUID.randomUUID()
            val id = UUID.randomUUID()
            val saksbehandlerIdent = "Z111111"
            rapid.sendTestMessage(behovNyttFormat(fødselsnummer, vedtaksperiodeId, "FORLENGELSE", id))
            rapid.sendTestMessage(
                løsningNyttFormat(
                    fødselsnummer,
                    vedtaksperiodeId,
                    "FORLENGELSE",
                    id,
                    saksbehandlerIdent = saksbehandlerIdent,
                ),
            )

            assertEquals(saksbehandlerIdent, finnSaksbehandlerIdentitet(id))
        }

    @Test
    fun `defaulter til spesialist oid om det er en automatisk behandling`() =
        e2eTest {
            val fødselsnummer = "83497290"
            val vedtaksperiodeId = UUID.randomUUID()
            val id = UUID.randomUUID()
            val saksbehandlerIdent = "Automatisk behandlet"
            rapid.sendTestMessage(behovNyttFormat(fødselsnummer, vedtaksperiodeId, "FORLENGELSE", id))
            rapid.sendTestMessage(
                løsningNyttFormat(
                    fødselsnummer,
                    vedtaksperiodeId,
                    "FORLENGELSE",
                    id,
                    automatiskBehandlet = true,
                    saksbehandlerIdent = saksbehandlerIdent,
                ),
            )

            assertEquals(VedtaksperiodeBehandletRiver.SPESIALIST_OID, finnSaksbehandlerIdentitet(id))
        }

    @Test
    fun `godkjenningsbehov blir lest fra rapid`() =
        e2eTest {
            val fødselsnummer = "1243356"
            val vedtaksperiodeId = UUID.randomUUID()
            val id = UUID.randomUUID()
            rapid.sendTestMessage(behovNyttFormat(fødselsnummer, vedtaksperiodeId, "FORLENGELSE", id))
            rapid.sendTestMessage(løsningNyttFormat(fødselsnummer, vedtaksperiodeId, "FORLENGELSE", id))

            assertEquals(listOf(id.toString()), finnGodkjenningsbehovLøsning(id))
            assertEquals(listOf(id.toString()), finnGodkjenningsbehovLøsningBegrunnelse(id))
        }

    @Test
    fun `godkjenningsbehov blir lest fra rapid uten refusjonstype`() =
        e2eTest {
            val fødselsnummer = "1243356"
            val vedtaksperiodeId = UUID.randomUUID()
            val id = UUID.randomUUID()
            rapid.sendTestMessage(behovNyttFormat(fødselsnummer, vedtaksperiodeId, "FORLENGELSE", id))
            rapid.sendTestMessage(løsningNyttFormat(fødselsnummer, vedtaksperiodeId, "FORLENGELSE", id, refusjonstype = null))

            assertEquals(listOf(id.toString()), finnGodkjenningsbehovLøsning(id))
            assertEquals(listOf(id.toString()), finnGodkjenningsbehovLøsningBegrunnelse(id))
        }

    @Test
    fun `persisterer periodetype for nye godkjenninger`() =
        e2eTest {
            val id = UUID.randomUUID()
            rapid.sendTestMessage(behovNyttFormat("8756876", UUID.randomUUID(), "FORLENGELSE", id))

            assertEquals("FORLENGELSE", finnPeriodetype(id))
        }

    @Test
    fun `persisterer inntektskilde`() =
        e2eTest {
            val id1 = UUID.randomUUID()
            val id2 = UUID.randomUUID()

            val fødselsnummer = "83497290"

            rapid.sendTestMessage(behovNyttFormat(fødselsnummer, UUID.randomUUID(), "FORLENGELSE", id1, "EN_ARBEIDSGIVER"))
            rapid.sendTestMessage(
                behovNyttFormat(
                    fødselsnummer,
                    UUID.randomUUID(),
                    "FORLENGELSE",
                    id2,
                    "FLERE_ARBEIDSGIVERE",
                ),
            )

            assertEquals("EN_ARBEIDSGIVER", finnInntektskilde(id1))
            assertEquals("FLERE_ARBEIDSGIVERE", finnInntektskilde(id2))
        }

    @Test
    fun `lagrer kun behov en gang`() =
        e2eTest {
            val eventId = UUID.randomUUID()
            val vedtaksperiodeId = UUID.randomUUID()
            val behov = behovNyttFormat("colemak", vedtaksperiodeId, "FORLENGELSE", eventId)

            rapid.sendTestMessage(behov)
            rapid.sendTestMessage(behov)

            assertEquals(1, finnGodkjenningsbehov(vedtaksperiodeId).size)
        }

    @Test
    fun `lagrer kun løsning en gang`() =
        e2eTest {
            val eventId = UUID.randomUUID()
            val vedtaksperiodeId = UUID.randomUUID()

            rapid.sendTestMessage(behovNyttFormat("colemak", vedtaksperiodeId, "FORLENGELSE", eventId))

            val løsning = løsningNyttFormat("colemak", vedtaksperiodeId, "FORLENGELSE", eventId)
            rapid.sendTestMessage(løsning)
            rapid.sendTestMessage(løsning)

            assertEquals(1, finnGodkjenningsbehovLøsning(eventId).size)
            assertEquals(1, finnGodkjenningsbehovLøsningBegrunnelse(eventId).size)
        }

    @Test
    fun `lagrer kun tilstandsendring en gang`() =
        e2eTest {
            val vedtaksperiodeId = UUID.randomUUID()
            val id = UUID.randomUUID()
            rapid.sendTestMessage(vedtaksperiodeEndret(id = id, vedtaksperiodeId = vedtaksperiodeId))
            rapid.sendTestMessage(vedtaksperiodeEndret(id = id, vedtaksperiodeId = vedtaksperiodeId))

            assertEquals(1, finnVedtaksperiodeTilstandsendring(id).size)
        }

    private fun E2eTestApp.finnGodkjenninger(fødselsnummer: String) =
        sessionOf(dataSource).use { session ->
            session.run(
                queryOf("SELECT * FROM godkjenning WHERE fodselsnummer=?;", fødselsnummer)
                    .map { it.string("vedtaksperiode_id") }
                    .asList,
            )
        }

    private fun E2eTestApp.finnGodkjenningsbehovLøsning(id: UUID) =
        sessionOf(dataSource).use { session ->
            session.run(
                queryOf("SELECT * FROM godkjenningsbehov_losning WHERE id=?;", id)
                    .map { it.string("id") }
                    .asList,
            )
        }

    private fun E2eTestApp.finnGodkjenningsbehovLøsningBegrunnelse(id: UUID) =
        sessionOf(dataSource).use { session ->
            session.run(
                queryOf("SELECT * FROM godkjenningsbehov_losning_begrunnelse WHERE id=?;", id)
                    .map { it.string("id") }
                    .asList,
            )
        }

    private fun E2eTestApp.finnVedtaksperiodeTilstandsendring(id: UUID) =
        sessionOf(dataSource).use { session ->
            session.run(
                queryOf("SELECT * FROM vedtaksperiode_tilstandsendring WHERE id=?;", id)
                    .map { it.string("tilstand_til") }
                    .asList,
            )
        }

    private fun E2eTestApp.finnPeriodetype(id: UUID) =
        sessionOf(dataSource).use { session ->
            session.run(
                queryOf("SELECT * FROM godkjenningsbehov WHERE id=?;", id)
                    .map { it.stringOrNull("periodetype") }
                    .asSingle,
            )
        }

    private fun E2eTestApp.finnSaksbehandlerIdentitet(id: UUID) =
        sessionOf(dataSource).use { session ->
            session.run(
                queryOf("SELECT * FROM godkjenningsbehov_losning WHERE id=?;", id)
                    .map { it.string("godkjent_av") }
                    .asSingle,
            )
        }

    private fun E2eTestApp.finnInntektskilde(id: UUID) =
        sessionOf(dataSource).use { session ->
            session.run(
                queryOf("SELECT * FROM godkjenningsbehov WHERE id=?;", id)
                    .map { it.stringOrNull("inntektskilde") }
                    .asSingle,
            )
        }

    private fun E2eTestApp.finnGodkjenningsbehov(vedtaksperiodeId: UUID) =
        sessionOf(dataSource).use { session ->
            session.run(
                queryOf("SELECT * FROM godkjenningsbehov WHERE vedtaksperiode_id=?;", vedtaksperiodeId)
                    .map { it.string("id") }
                    .asList,
            )
        }
}
