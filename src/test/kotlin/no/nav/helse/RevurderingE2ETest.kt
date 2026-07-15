package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.E2eTestApp.Companion.e2eTest
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*

class RevurderingE2ETest {
    @Test
    fun `lagrer i databasen`() =
        e2eTest {
            val revurderingId = UUID.randomUUID()
            val vedtaksperiodeId = UUID.randomUUID()
            val vedtaksperiodeId2 = UUID.randomUUID()

            rapid.sendTestMessage(revurderingIgangsatt(revurderingId, vedtaksperiodeId, vedtaksperiodeId2))
            assertEquals(1, tellRevurdering())

            rapid.sendTestMessage(Godkjenningsløsning(vedtaksperiodeId))
            assertEquals("IKKE_FERDIG", statusForRevurdering(revurderingId))

            rapid.sendTestMessage(Godkjenningsløsning(vedtaksperiodeId2))
            assertEquals("FERDIGSTILT_AUTOMATISK", statusForRevurdering(revurderingId))

            val vedtaksperioder = revurderingVedtaksperioder()
            assertEquals(
                setOf(
                    RevurderingVedtaksperiode(
                        fom = LocalDate.parse("2022-11-07"),
                        tom = LocalDate.parse("2022-11-29"),
                        skjæringstidspunkt = LocalDate.parse("2022-10-03"),
                        status = "FERDIGSTILT_AUTOMATISK",
                    ),
                    RevurderingVedtaksperiode(
                        fom = LocalDate.parse("2022-11-30"),
                        tom = LocalDate.parse("2022-12-15"),
                        skjæringstidspunkt = LocalDate.parse("2022-10-03"),
                        status = "FERDIGSTILT_AUTOMATISK",
                    ),
                ),
                vedtaksperioder.toSet(),
            )
        }

    private fun E2eTestApp.tellRevurdering(): Int =
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT COUNT(*) FROM revurdering"
            requireNotNull(
                session.run(queryOf(query).map { row -> row.int(1) }.asSingle),
            )
        }

    private fun E2eTestApp.statusForRevurdering(id: UUID): String =
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT status FROM revurdering where id='$id' LIMIT 1;"
            requireNotNull(
                session.run(queryOf(query).map { row -> row.string(1) }.asSingle),
            )
        }

    private fun E2eTestApp.revurderingVedtaksperioder(): List<RevurderingVedtaksperiode> =
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT status, periode_fom, periode_tom, skjaeringstidspunkt FROM revurdering_vedtaksperiode"
            requireNotNull(
                session.run(
                    queryOf(query)
                        .map { row ->
                            RevurderingVedtaksperiode(
                                fom = row.localDate("periode_fom"),
                                tom = row.localDate("periode_tom"),
                                skjæringstidspunkt = row.localDate("skjaeringstidspunkt"),
                                status = row.string("status"),
                            )
                        }.asList,
                ),
            )
        }

    private data class RevurderingVedtaksperiode(
        val fom: LocalDate,
        val tom: LocalDate,
        val skjæringstidspunkt: LocalDate,
        val status: String,
    )

    @Language("JSON")
    fun revurderingIgangsatt(
        revurderingId: UUID,
        vedtaksperiodeId: UUID,
        vedtaksperiodeId2: UUID,
        kilde: UUID = UUID.randomUUID(),
        årsak: String = "KORRIGERT_SØKNAD",
    ) = """{
        "@event_name":"overstyring_igangsatt",
        "fødselsnummer":"fnr",
        "revurderingId": "$revurderingId",
        "kilde":"$kilde",
        "skjæringstidspunkt":"2022-10-03",
        "periodeForEndringFom":"2022-11-07",
        "periodeForEndringTom":"2022-11-30",
        "årsak":"$årsak",
        "typeEndring": "REVURDERING",
        "berørtePerioder":[
            {
                "vedtaksperiodeId": "$vedtaksperiodeId",
                "skjæringstidspunkt":"2022-10-03",
                "periodeFom":"2022-11-07",
                "periodeTom":"2022-11-29",
                "orgnummer":"456",
                "typeEndring": "REVURDERING"
            },            
            {
                "vedtaksperiodeId":"$vedtaksperiodeId2",
                "skjæringstidspunkt":"2022-10-03",
                "periodeFom":"2022-11-30",
                "periodeTom":"2022-12-15",
                "orgnummer":"456",
                "typeEndring": "REVURDERING"
            }
          ],
        "@id":"69cf0c28-16d9-464e-bc71-bd9eabea22a1",
        "@opprettet":"2022-12-06T15:44:57.01089295"
    }
    """

    @Language("JSON")
    fun Godkjenningsløsning(
        vedtaksperiodeId: UUID,
    ) = """{
        "@event_name":"behov",
        "@behov": ["Godkjenning"],
        "Godkjenning": {
            "tags": ["Revurdering"]
        },
        "@løsning": {
            "Godkjenning": {
                "godkjent": true,
                "saksbehandlerIdent": "Z123456",
                "godkjentTidspunkt": "${LocalDateTime.now()}",
                "automatiskBehandling": true,
                "årsak": null,
                "begrunnelser": null,
                "kommentar": null
            }
        },
        "vedtaksperiodeId": "$vedtaksperiodeId",
        "@id": "${UUID.randomUUID()}",
        "@opprettet":"${LocalDateTime.now()}"
    }
    """
}
