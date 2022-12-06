package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.LocalDateTime
import java.util.UUID

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GodkjenningDaoTest {
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
    fun `lagrer begrunnelser for godkjenninger`() {
        val løsning = GodkjenningLøsningRiver(
            vedtaksperiodeId = UUID.randomUUID(),
            aktørId = "aktørId",
            fødselsnummer = "fødselsnummer",
            periodetype = "FORLENGELSE",
            inntektskilde = "EN_ARBEIDSGIVER",
            godkjenning = GodkjenningLøsningRiver.Godkjenning(
                godkjent = false,
                saksbehandlerIdent = "Z999999",
                godkjentTidspunkt = LocalDateTime.now(),
                årsak = "Annet",
                begrunnelser = listOf("Begrunnelse", "Begrunnelse2", "Annet"),
                kommentar = "Feil første fraværsdato"
            ),
            utbetalingType = "UTBETALING"
        )

        dataSource.insertGodkjenning(løsning)

        val godkjenninger = finnGodkjenninger(løsning.vedtaksperiodeId)
        assertEquals(1, godkjenninger.size)
        assertEquals(løsning.godkjenning.begrunnelser, finnBegrunnelser(godkjenninger.first().id))
    }

    private fun finnGodkjenninger(vedtaksperiodeId: UUID) = using(sessionOf(dataSource)) { session ->
        session.run(
            queryOf("SELECT * FROM godkjenning WHERE vedtaksperiode_id=?;", vedtaksperiodeId)
                .map {
                    TestGodkjenningDto(
                        id = it.long("id"),
                        kommentar = it.stringOrNull("kommentar")
                    )
                }
                .asList)
    }

    private fun finnWarnings(id: Long) = using(sessionOf(dataSource)) { session ->
        session.run(
            queryOf("SELECT * FROM warning_for_godkjenning WHERE godkjenning_ref=?;", id)
                .map {
                    it.string("tekst")
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

    data class TestGodkjenningDto(
        val id: Long,
        val kommentar: String?
    )
}
