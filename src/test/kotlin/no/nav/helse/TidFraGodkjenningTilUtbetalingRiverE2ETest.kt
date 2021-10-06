package no.nav.helse

import io.micrometer.prometheus.PrometheusMeterRegistry
import io.prometheus.client.Collector
import io.prometheus.client.CollectorRegistry
import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.LocalDateTime
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TidFraGodkjenningTilUtbetalingRiverE2ETest {
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
    fun `man kan regne ut tiden mellom godkjenning og utbetaling`() {
        val vedtaksperiodeId = UUID.randomUUID()
        insertGodkjenning(vedtaksperiodeId)
        river.sendTestMessage(vedtaksperiodeAvsluttet(vedtaksperiodeId))
        Assertions.assertEquals(1.0, CollectorRegistry.defaultRegistry.getSampleValue("tidBrukt_count"))
    }

    @Test
    fun `Uten godkjenning blir det ingen måling`() {
        val vedtaksperiodeId = UUID.randomUUID()
        river.sendTestMessage(vedtaksperiodeAvsluttet(vedtaksperiodeId))
        Assertions.assertEquals(0.0, CollectorRegistry.defaultRegistry.getSampleValue("tidBrukt_count"))
    }

    fun insertGodkjenning(vedtaksperiodeId: UUID) {
        using(sessionOf(dataSource)) { session ->
            session.run(
                queryOf(
                    """
                        INSERT INTO godkjenning(
                            vedtaksperiode_id,
                            aktor_id,
                            fodselsnummer,
                            godkjent_av,
                            godkjent_tidspunkt,
                            godkjent,
                            arsak,
                            kommentar,
                            periodetype,
                            inntektskilde
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    vedtaksperiodeId,
                    "123",
                    "112",
                    "123",
                    LocalDateTime.now(),
                    true,
                    "123",
                    "123",
                    "123",
                    "123"
                ).asUpdate
            )
        }
    }


    @Language("JSON")
    private fun vedtaksperiodeAvsluttet(vedtaksperiodeId: UUID) = """
    {
          "gjeldendeTilstand": "AVSLUTTET",
          "forrigeTilstand": "TIL_UTBETALING",
          "aktivitetslogg": {
            "aktiviteter": [
              {
                "kontekster": [
                  0,
                  1
                ],
                "alvorlighetsgrad": "INFO",
                "melding": "Behandler utbetaling",
                "detaljer": {},
                "tidsstempel": "2021-09-28 10:29:11.234"
              },
              {
                "kontekster": [
                  0,
                  1,
                  2,
                  3,
                  2,
                  4,
                  5
                ],
                "alvorlighetsgrad": "INFO",
                "melding": "OK fra Oppdragssystemet",
                "detaljer": {},
                "tidsstempel": "2021-09-28 10:29:11.235"
              }
            ],
            "kontekster": [
              {
                "kontekstType": "UtbetalingHendelse",
                "kontekstMap": {
                  "meldingsreferanseId": "905a3d8e-5269-412e-8b6e-2198fa37a60e",
                  "aktørId": "2285145663954",
                  "fødselsnummer": "03058022887",
                  "organisasjonsnummer": "972674818"
                }
              },
              {
                "kontekstType": "Person",
                "kontekstMap": {
                  "fødselsnummer": "03058022887",
                  "aktørId": "2285145663954"
                }
              },
              {
                "kontekstType": "Arbeidsgiver",
                "kontekstMap": {
                  "organisasjonsnummer": "972674818"
                }
              },
              {
                "kontekstType": "Utbetaling",
                "kontekstMap": {
                  "utbetalingId": "73004aa5-d83c-44ab-932a-cd15b716f108"
                }
              },
              {
                "kontekstType": "Vedtaksperiode",
                "kontekstMap": {
                  "vedtaksperiodeId": "e082b461-d0dd-4047-b375-64fc7eea829c"
                }
              },
              {
                "kontekstType": "Tilstand",
                "kontekstMap": {
                  "tilstand": "TIL_UTBETALING"
                }
              }
            ]
          },
          "harVedtaksperiodeWarnings": true,
          "hendelser": [
            "0b65f118-ff89-4721-91b4-54cf35b466db",
            "45eabd15-2d23-48e5-ac88-827b4f53ea30",
            "72f45a2f-da89-4f1e-84ba-33790093c76d"
          ],
          "makstid": "+999999999-12-31T23:59:59.999999999",
          "system_read_count": 1,
          "system_participating_services": [
            {
              "service": "spleis",
              "instance": "spleis-699cbddd9-68x7p",
              "time": "2021-09-28T10:29:11.235364622"
            },
            {
              "service": "spetakkel",
              "instance": "spetakkel-74c8f78c89-8jwlm",
              "time": "2021-09-28T10:29:11.305774949"
            }
          ],
          "@event_name": "vedtaksperiode_endret",
          "@id": "3f3046d4-2687-4ad1-a1d8-569f49bffabf",
          "@opprettet": "2021-09-28T10:29:11.235394277",
          "@forårsaket_av": {
            "behov": [
              "Utbetaling"
            ],
            "event_name": "behov",
            "id": "905a3d8e-5269-412e-8b6e-2198fa37a60e",
            "opprettet": "2021-09-28T10:29:10.69840093"
          },
          "fødselsnummer": "03058022887",
          "aktørId": "2285145663954",
          "organisasjonsnummer": "972674818",
          "vedtaksperiodeId": "$vedtaksperiodeId"
        }
    """
}
