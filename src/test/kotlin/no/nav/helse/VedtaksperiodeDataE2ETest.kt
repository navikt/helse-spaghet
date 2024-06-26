package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.LocalDate
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class VedtaksperiodeDataE2ETest {
    private val embeddedPostgres = embeddedPostgres()
    private val dataSource = setupDataSourceMedFlyway(embeddedPostgres)
    private val river = TestRapid()
        .setupRivers(dataSource)

    @AfterAll
    fun tearDown() {
        river.stop()
        dataSource.connection.close()
        embeddedPostgres.close()
    }


    @Test
    fun `send en enkel melding, lagre en enkelt periode`() {
        val id = UUID.randomUUID()
        river.sendTestMessage(vedtakOpprettet(
            id,
            "01010112345",
            "12345",
            "et organisasjonsnummer",
            LocalDate.of(2020, 1, 1),
            LocalDate.of(2020, 1, 31),
            LocalDate.of(2020, 1, 1)
        ))

        assertFelt(
            id,
            forventetFnr = "01010112345",
            forventetAktørId = "12345",
            forventetYrkesaktivitet = "et organisasjonsnummer",
            forventetFom = LocalDate.of(2020, 1, 1),
            forventetTom = LocalDate.of(2020, 1, 31),
            forventetSkjæringstidspunkt = LocalDate.of(2020, 1, 1),
            forventetTilstand = "START"
        )
    }

    @Test
    fun `send samme periode igjen, men annen data`() {
        val id = UUID.randomUUID()
        river.sendTestMessage(vedtakOpprettet(
            vedtaksperiodeId = id,
            fnr = "01010112345",
            aktørId = "12345",
            yrkesaktivitet = "et organisasjonsnummer",
            fom = LocalDate.of(2020, 1, 1),
            tom = LocalDate.of(2020, 1, 31),
            skjæringstidspunkt = LocalDate.of(2020, 1, 1)
        ))
        river.sendTestMessage(vedtakEndret(
            vedtaksperiodeId = id,
            fnr = "99999999999",
            aktørId = "99999",
            yrkesaktivitet = "kølgruvene",
            fom = LocalDate.of(2029, 1, 1),
            tom = LocalDate.of(2029, 1, 31),
            skjæringstidspunkt = LocalDate.of(2029, 1, 1),
            tilstand = "AVSLUTTET_UTEN_UTBETALING"
        ))

        assertFelt(
            vedtaksperiodeId = id,
            forventetFnr = "99999999999",
            forventetAktørId = "99999",
            forventetYrkesaktivitet = "kølgruvene",
            forventetFom = LocalDate.of(2029, 1, 1),
            forventetTom = LocalDate.of(2029, 1, 31),
            forventetSkjæringstidspunkt = LocalDate.of(2029, 1, 1),
            forventetTilstand = "AVSLUTTET_UTEN_UTBETALING"
        )
    }

    @Test
    fun `send person avstemt`() {
        val første = UUID.randomUUID()
        val andre = UUID.randomUUID()
        val tredje = UUID.randomUUID()
        river.sendTestMessage(personAvstemt(første, andre, tredje))
        assertFelt(
            første,
            forventetFnr = "1",
            forventetAktørId = "2",
            forventetYrkesaktivitet = "12",
            forventetSkjæringstidspunkt = LocalDate.of(2001, 1, 1),
            forventetTilstand = "START",
            forventetFom = LocalDate.of(2001, 1, 1),
            forventetTom = LocalDate.of(2001, 1, 11)
        )
        assertFelt(
            andre,
            forventetFnr = "1",
            forventetAktørId = "2",
            forventetYrkesaktivitet = "12",
            forventetSkjæringstidspunkt = LocalDate.of(2002, 2, 2),
            forventetTilstand = "AVSLUTTET_UTEN_UTBETALING",
            forventetFom = LocalDate.of(2002, 2, 2),
            forventetTom = LocalDate.of(2002, 2, 22)
        )
        assertFelt(
            tredje,
            forventetFnr = "1",
            forventetAktørId = "2",
            forventetYrkesaktivitet = "3",
            forventetSkjæringstidspunkt = LocalDate.of(2003, 3, 3),
            forventetTilstand = "AVSLUTTET",
            forventetFom = LocalDate.of(2003, 3, 3),
            forventetTom = LocalDate.of(2003, 3, 13)
        )
    }

    private fun vedtakOpprettet(vedtaksperiodeId: UUID, fnr: String, aktørId: String, yrkesaktivitet: String, fom: LocalDate, tom: LocalDate, skjæringstidspunkt: LocalDate) = """
            {
            "@event_name": "vedtaksperiode_opprettet",
            "vedtaksperiodeId": "$vedtaksperiodeId",
            "fødselsnummer": "$fnr",
            "aktørId": "$aktørId",
            "organisasjonsnummer": "$yrkesaktivitet",
            "fom": "$fom",
            "tom": "$tom",
            "skjæringstidspunkt": "$skjæringstidspunkt"
            }
        """

    private fun vedtakEndret(vedtaksperiodeId: UUID, fnr: String, aktørId: String, yrkesaktivitet: String, fom: LocalDate, tom: LocalDate, skjæringstidspunkt: LocalDate, tilstand: String) = """
            {
            "@event_name": "vedtaksperiode_endret",
            "vedtaksperiodeId": "$vedtaksperiodeId",
            "fødselsnummer": "$fnr",
            "aktørId": "$aktørId",
            "organisasjonsnummer": "$yrkesaktivitet",
            "fom": "$fom",
            "tom": "$tom",
            "skjæringstidspunkt": "$skjæringstidspunkt",
            "gjeldendeTilstand": "$tilstand"
            }
        """

    @Language("JSON")
    private fun personAvstemt(første: UUID, andre: UUID, tredje: UUID): String = """
  {
    "@event_name": "person_avstemt",
    "fødselsnummer": "1",
    "aktørId": "2",
    "arbeidsgivere": [{
        "organisasjonsnummer":"12",
        "vedtaksperioder": [{
            "id": "$første",
            "tilstand": "START",
            "oppdatert": "2001-01-01T11:11:11.111111",
            "fom": "2001-01-01",
            "tom": "2001-01-11",
            "skjæringstidspunkt": "2001-01-01"
        },{
            "id": "$andre",
            "tilstand": "AVSLUTTET_UTEN_UTBETALING",
            "oppdatert": "2002-02-02T22:22:22.222222",
            "fom": "2002-02-02",
            "tom": "2002-02-22",
            "skjæringstidspunkt": "2002-02-02"
        }]
    },
    {
        "organisasjonsnummer":"3",
        "vedtaksperioder": [{
            "id": "$tredje",
            "tilstand": "AVSLUTTET",
            "oppdatert": "2003-03-03T13:13:13.131313",
            "fom": "2003-03-03",
            "tom": "2003-03-13",
            "skjæringstidspunkt": "2003-03-03"
        }]
    }
]}
    """.trimIndent()

    private fun assertFelt(
        vedtaksperiodeId: UUID,
        forventetFnr: String,
        forventetAktørId: String,
        forventetYrkesaktivitet: String,
        forventetFom: LocalDate,
        forventetTom: LocalDate,
        forventetSkjæringstidspunkt: LocalDate,
        forventetTilstand: String
    ) {
        val query = """select * from vedtaksperiode_data where vedtaksperiodeId = :vedtaksperiodeId"""
        sessionOf(dataSource).use { session ->
            val actuals: Map<String, Any> = session.run(
                queryOf(query, mapOf("vedtaksperiodeId" to vedtaksperiodeId))
                .map {
                    mapOf("fnr" to it.string("fnr"),
                        "aktørId" to it.string("aktorId"),
                        "yrkesaktivitet" to it.string("yrkesaktivitet"),
                        "fom" to it.localDate("fom"),
                        "tom" to it.localDate("tom"),
                        "tilstand" to it.string("tilstand"),
                        "skjæringstidspunkt" to it.localDate("skjaeringstidspunkt"))
                }.asSingle)!!

            assertEquals(forventetFnr, actuals["fnr"]!!)
            assertEquals(forventetAktørId, actuals["aktørId"]!!)
            assertEquals(forventetYrkesaktivitet, actuals["yrkesaktivitet"]!!)
            assertEquals(forventetFom, actuals["fom"]!!)
            assertEquals(forventetTom, actuals["tom"]!!)
            assertEquals(forventetSkjæringstidspunkt, actuals["skjæringstidspunkt"]!!)
            assertEquals(forventetTilstand, actuals["tilstand"]!!)
        }
    }
}
