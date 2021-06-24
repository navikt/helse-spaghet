package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.E2eTestApp.Companion.e2eTest
import no.nav.helse.TestData.aktivitet
import no.nav.helse.TestData.vedtaksperiodeEndret
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
            val endring = vedtaksperiodeEndret.aktivitet(
                aktivitet
                    .melding("Utbetaling skal gå rett til bruker")
                    .error()
            )
            endring.sendTilRapid()

            assertEquals(endring.aktiviteter.map { it.melding }, hentErrors(endring.vedtaksperiodeId))
        }
    }

    @Test
    fun `to errors`() {
        e2eTest {
            val endring = vedtaksperiodeEndret
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
            val endring = vedtaksperiodeEndret
                .aktivitet(
                    aktivitet
                        .melding("Bruker skal gå rett til start")
                        .info()
                )
            endring.sendTilRapid()

            assertEquals(endring.aktiviteter.map { it.melding }, hentErrors(endring.vedtaksperiodeId))
        }
    }

    @Test
    fun `lagrer tilstandsendring`() {
        e2eTest {
            val endring = vedtaksperiodeEndret
                .forrigeTilstand("AVVENTER_GAP")
                .gjeldendeTilstand("TIL_INFOTRYGD")
                .kontekstType("Utbetalingshistorikk")

            endring.sendTilRapid()

            val tilstandsendringer = hentTilstandsendringer(endring.vedtaksperiodeId)

            assertEquals(
                listOf(
                    Tilstandsendring(
                        vedtaksperiodeId = endring.vedtaksperiodeId,
                        tidsstempel = endring.opprettet,
                        tilstandFra = endring.forrigeTilstand,
                        tilstandTil = endring.gjeldendeTilstand,
                        kilde = endring.forårsaketAv,
                        kildeType = endring.kontekstType!!
                    )
                ), tilstandsendringer
            )
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

    private fun E2eTestApp.hentTilstandsendringer(vedtaksperiodeId: UUID) =
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT * FROM vedtaksperiode_tilstandsendring WHERE vedtaksperiode_id=?;"
            session.run(queryOf(query, vedtaksperiodeId)
                .map { row ->
                    Tilstandsendring(
                        vedtaksperiodeId = UUID.fromString(row.string("vedtaksperiode_id")),
                        tidsstempel = row.localDateTime("tidsstempel"),
                        tilstandFra = row.string("tilstand_fra"),
                        tilstandTil = row.string("tilstand_til"),
                        kilde = UUID.fromString(row.string("kilde")),
                        kildeType = row.string("kilde_type")
                    )
                }
                .asList
            )
        }

    private data class Tilstandsendring(
        val vedtaksperiodeId: UUID,
        val tidsstempel: LocalDateTime,
        val tilstandFra: String,
        val tilstandTil: String,
        val kilde: UUID,
        val kildeType: String
    )
}
