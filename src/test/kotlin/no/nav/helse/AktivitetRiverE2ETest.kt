package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.LocalDateTime
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AktivitetRiverE2ETest {
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
    fun `lagrer errors i database`() {
        val vedtaksperiodeId = UUID.randomUUID()
        river.sendTestMessage(vedtaksperiodeEndretMedError(vedtaksperiodeId, UUID.randomUUID()))

        assertEquals(listOf("Utbetaling skal gå rett til bruker"), hentErrors(vedtaksperiodeId))
    }

    @Test
    fun `lagrer tilstandsendring`() {
        val tidspunkt = LocalDateTime.parse("2020-10-15T13:37:10.555")
        val vedtaksperiodeId = UUID.randomUUID()
        val kilde = UUID.randomUUID()
        river.sendTestMessage(vedtaksperiodeEndretMedError(vedtaksperiodeId, kilde))

        val tilstandsendringer = hentTilstandsendringer(vedtaksperiodeId)

        assertEquals(listOf(
                Tilstandsendring(
                        vedtaksperiodeId = vedtaksperiodeId,
                        tidsstempel = tidspunkt,
                        tilstandFra = "AVVENTER_GAP",
                        tilstandTil = "TIL_INFOTRYGD",
                        kilde = kilde,
                        kildeType = "Utbetalingshistorikk"
                )
        ), tilstandsendringer)

    }

    private fun hentErrors(vedtaksperiodeId: UUID) =
            sessionOf(dataSource).use { session ->
                @Language("PostgreSQL")
                val query = """SELECT * FROM vedtaksperiode_aktivitet WHERE vedtaksperiode_id=?;"""
                session.run(queryOf(query, vedtaksperiodeId)
                        .map { it.string("melding") }
                        .asList
                )
            }

    private fun hentTilstandsendringer(vedtaksperiodeId: UUID) =
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

    @Language("JSON")
    private fun vedtaksperiodeEndretMedError(vedtaksperiodeId: UUID, kilde: UUID) = """
{
  "vedtaksperiodeId": "$vedtaksperiodeId",
  "organisasjonsnummer": "987654321",
  "gjeldendeTilstand": "TIL_INFOTRYGD",
  "forrigeTilstand": "AVVENTER_GAP",
  "aktivitetslogg": {
    "aktiviteter": [
      {
        "alvorlighetsgrad": "ERROR",
        "melding": "Utbetaling skal gå rett til bruker",
        "tidsstempel": "2020-10-15 13:37:10.555"
      }
    ],
    "kontekster": [
      {
        "kontekstType": "Utbetalingshistorikk",
        "kontekstMap": {
          "aktørId": "1000000000096",
          "fødselsnummer": "10101010100",
          "organisasjonsnummer": "987654321",
          "id": "$kilde"
        }
      },
      {
        "kontekstType": "Person",
        "kontekstMap": {
          "fødselsnummer": "10101010100",
          "aktørId": "1000000000096"
        }
      },
      {
        "kontekstType": "Arbeidsgiver",
        "kontekstMap": {
          "organisasjonsnummer": "987654321"
        }
      },
      {
        "kontekstType": "Vedtaksperiode",
        "kontekstMap": {
          "vedtaksperiodeId": "cd270fa4-19f2-4e69-85a6-b332b53ff2b3"
        }
      },
      {
        "kontekstType": "Tilstand",
        "kontekstMap": {
          "tilstand": "AVVENTER_GAP"
        }
      }
    ]
  },
  "@event_name": "vedtaksperiode_endret",
  "@id": "afaa7641-8215-4940-a984-cbefec7ac705",
  "@opprettet": "2020-10-15T13:37:10.555",
  "@forårsaket_av": {
    "event_name": "behov",
    "id": "$kilde",
    "opprettet": "2020-10-15T13:35:10.42069"
  },
  "aktørId": "1000000000096",
  "fødselsnummer": "10101010100"
}
    """
}
