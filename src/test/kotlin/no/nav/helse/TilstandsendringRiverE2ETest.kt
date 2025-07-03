package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.E2eTestApp.Companion.e2eTest
import no.nav.helse.TestData.vedtaksperiodeEndret
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.util.*

class TilstandsendringRiverE2ETest {

    @Test
    fun `lagrer tilstandsendring`() {
        e2eTest {
            val endring = vedtaksperiodeEndret
                .forrigeTilstand("AVVENTER_GAP")
                .gjeldendeTilstand("TIL_INFOTRYGD")
                .kildeType("behov")

            endring.sendTilRapid()

            val tilstandsendringer = hentTilstandsendringer(endring.vedtaksperiodeId)

            assertEquals(
                listOf(
                    Tilstandsendring(
                        vedtaksperiodeId = endring.vedtaksperiodeId,
                        behandlingId = endring.behandlingId,
                        tidsstempel = endring.opprettet,
                        tilstandFra = endring.forrigeTilstand,
                        tilstandTil = endring.gjeldendeTilstand,
                        kilde = endring.forårsaketAv,
                        kildeType = endring.kildeType!!
                    )
                ), tilstandsendringer
            )
        }

    }

    private fun E2eTestApp.hentTilstandsendringer(vedtaksperiodeId: UUID) =
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT * FROM vedtaksperiode_tilstandsendring WHERE vedtaksperiode_id=?;"
            session.run(queryOf(query, vedtaksperiodeId)
                .map { row ->
                    Tilstandsendring(
                        vedtaksperiodeId = UUID.fromString(row.string("vedtaksperiode_id")),
                        behandlingId = UUID.fromString(row.string("behandling_id")),
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
        val behandlingId: UUID,
        val tidsstempel: LocalDateTime,
        val tilstandFra: String,
        val tilstandTil: String,
        val kilde: UUID,
        val kildeType: String
    )
}
