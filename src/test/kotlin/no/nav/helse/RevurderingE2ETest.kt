package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.intellij.lang.annotations.Language
import java.time.LocalDate
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.LocalDateTime
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RevurderingE2ETest {
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
    fun `lagrer i databasen`() {
        val revurderingId = UUID.randomUUID()
        river.sendTestMessage(revurderingIgangsatt(revurderingId))
        river.sendTestMessage(revurderingFerdigstilt(revurderingId))
        assertEquals(1, tellRevurdering())
        assertEquals("FERDIGSTILT_AUTOMATISK", statusForRevurdering(revurderingId))
        val vedtaksperioder = revurderingVedtaksperioder()
        assertEquals(setOf(
            RevurderingVedtaksperiode(
                fom = LocalDate.parse("2022-11-07"),
                tom = LocalDate.parse("2022-11-29"),
                skjæringstidspunkt = LocalDate.parse("2022-10-03"),
                status = "FERDIGSTILT_AUTOMATISK"
            ),
            RevurderingVedtaksperiode(
                fom = LocalDate.parse("2022-11-30"),
                tom = LocalDate.parse("2022-12-15"),
                skjæringstidspunkt = LocalDate.parse("2022-10-03"),
                status = "FERDIGSTILT_AUTOMATISK"
            )
        ), vedtaksperioder.toSet())

    }


    private fun tellRevurdering(): Int {
        return sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT COUNT(*) FROM revurdering"
            requireNotNull(
                session.run(queryOf(query).map { row -> row.int(1) }.asSingle)
            )
        }
    }


    private fun statusForRevurdering(id: UUID): String {
        return sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT status FROM revurdering where id='$id' LIMIT 1;"
            requireNotNull(
                session.run(queryOf(query).map { row -> row.string(1) }.asSingle)
            )
        }
    }

    private fun revurderingVedtaksperioder(): List<RevurderingVedtaksperiode> {
        return sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT status, periode_fom, periode_tom, skjaeringstidspunkt FROM revurdering_vedtaksperiode"
            requireNotNull(
                session.run(queryOf(query).map { row ->
                    RevurderingVedtaksperiode(
                        fom = row.localDate("periode_fom"),
                        tom = row.localDate("periode_tom"),
                        skjæringstidspunkt = row.localDate("skjaeringstidspunkt"),
                        status = row.string("status")
                    )
                }.asList)
            )
        }
    }

    private data class RevurderingVedtaksperiode(
        val fom: LocalDate,
        val tom: LocalDate,
        val skjæringstidspunkt: LocalDate,
        val status: String
    )


    @Language("JSON")
    fun revurderingIgangsatt(
        revurderingId: UUID,
        kilde: UUID = UUID.randomUUID(),
        årsak: String = "KORRIGERT_SØKNAD"
    ) = """{
        "@event_name":"overstyring_igangsatt",
        "fødselsnummer":"fnr",
        "aktørId":"aktorId",
        "revurderingId": "$revurderingId",
        "kilde":"$kilde",
        "skjæringstidspunkt":"2022-10-03",
        "periodeForEndringFom":"2022-11-07",
        "periodeForEndringTom":"2022-11-30",
        "årsak":"$årsak",
        "typeEndring": "REVURDERING",
        "berørtePerioder":[
            {
                "vedtaksperiodeId":"c0f78b58-4687-4191-adf8-6588c5982abb",
                "skjæringstidspunkt":"2022-10-03",
                "periodeFom":"2022-11-07",
                "periodeTom":"2022-11-29",
                "orgnummer":"456",
                "typeEndring": "REVURDERING"
            },            
            {
                "vedtaksperiodeId":"c0c78b58-4687-4191-adf8-6588c5982abb",
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
    fun revurderingFerdigstilt(
        revurderingId: UUID,
        status: String = "FERDIGSTILT_AUTOMATISK"
    ) = """{
        "@event_name":"revurdering_ferdigstilt",
        "revurderingId": "$revurderingId",
        "status": "$status",
        "berørtePerioder":[
            {
                "vedtaksperiodeId":"c0f78b58-4687-4191-adf8-6588c5982abb",
                "status": "$status"
            },            
            {
                "vedtaksperiodeId":"c0c78b58-4687-4191-adf8-6588c5982abb",
                "status": "$status"
            }
          ],
        "@id": "${UUID.randomUUID()}",
        "@opprettet":"${LocalDateTime.now()}"
    }
    """

}