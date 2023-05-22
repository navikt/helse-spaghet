package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import no.nav.helse.Util.uuid
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.LocalDateTime
import java.util.UUID

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GodkjenningDaoTest {
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
    fun `lagrer begrunnelser for godkjenninger`() {
        val løsning = Godkjenningsbehov(
            vedtaksperiodeId = UUID.randomUUID(),
            aktørId = "aktørId",
            fødselsnummer = "fødselsnummer",
            periodetype = "FORLENGELSE",
            inntektskilde = "EN_ARBEIDSGIVER",
            løsning = Godkjenningsbehov.Løsning(
                godkjent = false,
                saksbehandlerIdent = "Z999999",
                godkjentTidspunkt = LocalDateTime.now(),
                årsak = "Annet",
                begrunnelser = listOf("Begrunnelse", "Begrunnelse2", "Annet"),
                kommentar = "Feil første fraværsdato"
            ),
            utbetalingType = "UTBETALING",
            refusjonType = "FULL_REFUSJON",
            saksbehandleroverstyringer = emptyList()
        )

        dataSource.insertGodkjenning(løsning)

        val godkjenninger = finnGodkjenninger(løsning.vedtaksperiodeId)
        assertEquals(1, godkjenninger.size)
        assertEquals(løsning.løsning.begrunnelser, finnBegrunnelser(godkjenninger.first().id))
        assertFalse(godkjenninger.first().erSaksbehandleroverstyringer)
        assertEquals(emptyList<UUID>(), finnGodkjenningOverstyringer(godkjenninger.first().id))
    }

    @Test
    fun `lagrer saksbehandleroverstyringer for godkjenning`() {
        val saksbehandleroverstyringer = listOf(UUID.randomUUID())
        val løsning = Godkjenningsbehov(
            vedtaksperiodeId = UUID.randomUUID(),
            aktørId = "aktørId",
            fødselsnummer = "fødselsnummer",
            periodetype = "FORLENGELSE",
            inntektskilde = "EN_ARBEIDSGIVER",
            løsning = Godkjenningsbehov.Løsning(
                godkjent = false,
                saksbehandlerIdent = "Z999999",
                godkjentTidspunkt = LocalDateTime.now(),
                årsak = "Annet",
                begrunnelser = listOf("Begrunnelse", "Begrunnelse2", "Annet"),
                kommentar = "Feil første fraværsdato"
            ),
            utbetalingType = "UTBETALING",
            refusjonType = "FULL_REFUSJON",
            saksbehandleroverstyringer = saksbehandleroverstyringer
        )

        dataSource.insertGodkjenning(løsning)

        val godkjenninger = finnGodkjenninger(løsning.vedtaksperiodeId)
        assertEquals(1, godkjenninger.size)
        assertTrue(godkjenninger.first().erSaksbehandleroverstyringer)
        assertEquals(saksbehandleroverstyringer, finnGodkjenningOverstyringer(godkjenninger.first().id))
    }

    private fun finnGodkjenninger(vedtaksperiodeId: UUID) = using(sessionOf(dataSource)) { session ->
        session.run(
            queryOf("SELECT * FROM godkjenning WHERE vedtaksperiode_id=?;", vedtaksperiodeId)
                .map {
                    TestGodkjenningDto(
                        id = it.long("id"),
                        kommentar = it.stringOrNull("kommentar"),
                        erSaksbehandleroverstyringer = it.boolean("er_saksbehandleroverstyringer")
                    )
                }
                .asList)
    }

    private fun finnBegrunnelser(id: Long) = using(sessionOf(dataSource)) { session ->
        session.run(
            queryOf("SELECT * FROM begrunnelse WHERE godkjenning_ref=?;", id)
                .map {
                    it.string("tekst")
                }
                .asList)
    }

    private fun finnGodkjenningOverstyringer(id: Long) = using(sessionOf(dataSource)) { session ->
        session.run(
            queryOf("SELECT * FROM godkjenning_overstyringer WHERE godkjenning_ref=?;", id)
                .map {
                    it.uuid("overstyring_hendelse_id")
                }
                .asList)
    }

    data class TestGodkjenningDto(
        val id: Long,
        val kommentar: String?,
        val erSaksbehandleroverstyringer: Boolean
    )
}
