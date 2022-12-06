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
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RevurderingIgangsattE2ETest {
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
        river.sendTestMessage(revurderingIgangsatt())
        assertEquals(1, tellRevurderingIgangsatt())
        val vedtaksperioder = revurderingIgangsattVedtaksperioder()
        assertEquals(setOf(
            RevurderingIgangSattVedtaksperiode(
                fom = LocalDate.parse("2022-11-07"),
                tom = LocalDate.parse("2022-11-29"),
                skjæringstidspunkt = LocalDate.parse("2022-10-03")
            ),
            RevurderingIgangSattVedtaksperiode(
                fom = LocalDate.parse("2022-11-30"),
                tom = LocalDate.parse("2022-12-15"),
                skjæringstidspunkt = LocalDate.parse("2022-10-03")
            )
        ), vedtaksperioder.toSet())

    }


    private fun tellRevurderingIgangsatt(): Int {
        return sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT COUNT(*) FROM revurdering_igangsatt"
            requireNotNull(
                session.run(queryOf(query).map { row -> row.int(1) }.asSingle)
            )
        }
    }

    private fun revurderingIgangsattVedtaksperioder(): List<RevurderingIgangSattVedtaksperiode> {
        return sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT periode_fom, periode_tom, skjaeringstidspunkt FROM revurdering_igangsatt_vedtaksperiode"
            requireNotNull(
                session.run(queryOf(query).map { row ->
                    RevurderingIgangSattVedtaksperiode(
                        fom = row.localDate("periode_fom"),
                        tom = row.localDate("periode_tom"),
                        skjæringstidspunkt = row.localDate("skjaeringstidspunkt")
                    )
                }.asList)
            )
        }
    }

    private data class RevurderingIgangSattVedtaksperiode(
        val fom: LocalDate,
        val tom: LocalDate,
        val skjæringstidspunkt: LocalDate
    )


    @Language("JSON")
    fun revurderingIgangsatt(
        kilde: UUID = UUID.randomUUID(),
        årsak: String = "KORRIGERT_SØKNAD"
    ) = """{
        "@event_name":"revurdering_igangsatt",
        "fødselsnummer":"fnr",
        "aktørId":"aktorId",
        "kilde":"$kilde",
        "skjæringstidspunkt":"2022-10-03",
        "periodeForEndringFom":"2022-11-07",
        "periodeForEndringTom":"2022-11-30",
        "årsak":"$årsak",
        "berørtePerioder":[
            {
                "vedtaksperiodeId":"c0f78b58-4687-4191-adf8-6588c5982abb",
                "skjæringstidspunkt":"2022-10-03",
                "periodeFom":"2022-11-07",
                "periodeTom":"2022-11-29",
                "orgnummer":"456"
            },            
            {
                "vedtaksperiodeId":"c0c78b58-4687-4191-adf8-6588c5982abb",
                "skjæringstidspunkt":"2022-10-03",
                "periodeFom":"2022-11-30",
                "periodeTom":"2022-12-15",
                "orgnummer":"456"
            }
          ],
        "@id":"69cf0c28-16d9-464e-bc71-bd9eabea22a1",
        "@opprettet":"2022-12-06T15:44:57.01089295"
    }
    """

}