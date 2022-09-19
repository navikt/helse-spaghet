package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.E2eTestApp.Companion.e2eTest
import no.nav.helse.TestData.aktivitet
import no.nav.helse.TestData.nyAktivitet
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.LocalDateTime
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AktivitetRiverE2ETest {
    @Test
    fun `lagrer errors i database`() {
        e2eTest {
            val endring = nyAktivitet.aktivitet(
                aktivitet
                    .melding("Utbetaling skal gå rett til bruker")
                    .error()
            )
            endring.sendTilRapid()

            assertEquals(endring.aktiviteter.map { it.melding }, hentErrors(endring.vedtaksperiodeId))
        }
    }

    @Test
    fun `finner aktiviteter for hendelse`() {
        e2eTest {
            val endring = nyAktivitet.aktivitet(
                aktivitet
                    .melding("Behandler simulering")
                    .error()
            ).aktivitet(
                aktivitet
                    .melding("Simulering kom frem til et annet totalbeløp. Kontroller beløpet til utbetaling")
                    .error()
            )
            endring.sendTilRapid()

            val expected = listOf(
                "Behandler simulering",
                "Simulering kom frem til et annet totalbeløp. Kontroller beløpet til utbetaling"
            )
            assertEquals(expected, finnAktiviteter(endring.meldingsId))
            assertEquals(expected, finnAktiviteterKilde(endring.forårsaketAv))
        }
    }

    @Test
    fun `to errors`() {
        e2eTest {
            val endring = nyAktivitet
                .aktivitet(
                    aktivitet
                        .melding("Utbetaling skal gå rett til bruker")
                        .error()
                ).aktivitet(
                    aktivitet
                        .melding("Bruker skal gå rett til start")
                        .error()
                )
            endring.sendTilRapid()

            assertEquals(endring.aktiviteter.map { it.melding }, hentErrors(endring.vedtaksperiodeId))
        }
    }

    @Test
    fun `lagrer aktivitet uavhengig av nivå`() {
        e2eTest {
            val endring = nyAktivitet
                .aktivitet(
                    aktivitet
                        .melding("Bruker skal gå rett til start")
                        .info()
                )
            endring.sendTilRapid()

            assertEquals(endring.aktiviteter.map { it.melding }, hentErrors(endring.vedtaksperiodeId))
        }
    }

    private fun E2eTestApp.hentErrors(vedtaksperiodeId: UUID) =
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = """SELECT * FROM vedtaksperiode_aktivitet WHERE vedtaksperiode_id=?;"""
            session.run(queryOf(query, vedtaksperiodeId)
                .map { it.string("melding") }
                .asList
            )
        }

    private fun E2eTestApp.finnAktiviteter(id: UUID) = sessionOf(dataSource).use { session ->
        session.run(queryOf("SELECT * FROM vedtaksperiode_aktivitet WHERE id=?;", id)
            .map { it.string("melding") }
            .asList)
    }
    private fun E2eTestApp.finnAktiviteterKilde(kilde: UUID) = sessionOf(dataSource).use { session ->
        session.run(queryOf("SELECT * FROM vedtaksperiode_aktivitet WHERE kilde=?;", kilde)
            .map { it.string("melding") }
            .asList)
    }
}
