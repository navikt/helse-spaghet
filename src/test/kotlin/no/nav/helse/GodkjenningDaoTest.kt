package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.util.UUID

class GodkjenningDaoTest {
    val dataSource = setupDataSourceMedFlyway()

    @Test
    fun `legger til warnings og begrunnelser for godkjenninger`() {
        val løsning = GodkjenningLøsning(
            vedtaksperiodeId = UUID.randomUUID(),
            aktørId = "aktørId",
            fødselsnummer = "fødselsnummer",
            warnings = listOf("Test warning", "Test warning 2"),
            godkjenning = GodkjenningLøsning.Godkjenning(
                godkjent = false,
                saksbehandlerIdent = "Z999999",
                godkjentTidspunkt = LocalDateTime.now(),
                årsak = "Annet",
                begrunnelser = listOf("Begrunnelse", "Begrunnelse2", "Annet"),
                kommentar = "Feil første fraværsdato"
            )
        )

        dataSource.insertGodkjenning(løsning)

        val godkjenninger = finnGodkjenninger(løsning.vedtaksperiodeId)
        assertEquals(1, godkjenninger.size)
        assertEquals(løsning.warnings, finnWarnings(godkjenninger.first().id))
        assertEquals(løsning.godkjenning.begrunnelser, finnBegrunnelser(godkjenninger.first().id))
    }

    fun finnGodkjenninger(vedtaksperiodeId: UUID) = using(sessionOf(dataSource)) { session ->
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

    fun finnWarnings(id: Long) = using(sessionOf(dataSource)) { session ->
        session.run(
            queryOf("SELECT * FROM warning WHERE godkjenning_ref=?;", id)
                .map {
                    it.string("tekst")
                }
                .asList)
    }

    fun finnBegrunnelser(id: Long) = using(sessionOf(dataSource)) { session ->
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
