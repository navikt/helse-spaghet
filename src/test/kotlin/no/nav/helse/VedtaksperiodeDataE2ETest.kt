package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.E2eTestApp.Companion.e2eTest
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.util.*

class VedtaksperiodeDataE2ETest {
    @Test
    fun `send en enkel melding, lagre en enkelt periode for arbeidstaker`() =
        e2eTest {
            val id = UUID.randomUUID()
            rapid.sendTestMessage(
                vedtakOpprettet(
                    vedtaksperiodeId = id,
                    fnr = "01010112345",
                    yrkesaktivitet = "ARBEIDSTAKER",
                    orgnr = "et organisasjonsnummer",
                    fom = LocalDate.of(2020, 1, 1),
                    tom = LocalDate.of(2020, 1, 31),
                    skjæringstidspunkt = LocalDate.of(2020, 1, 1),
                ),
            )

            assertFelt(
                id,
                forventetFnr = "01010112345",
                forventetAktørId = standardAktørId,
                forventetYrkesaktivitet = "et organisasjonsnummer",
                forventetFom = LocalDate.of(2020, 1, 1),
                forventetTom = LocalDate.of(2020, 1, 31),
                forventetSkjæringstidspunkt = LocalDate.of(2020, 1, 1),
                forventetTilstand = "START",
            )
        }

    @Test
    fun `send en enkel melding, lagre en enkelt periode for selvstendig`() =
        e2eTest {
            val id = UUID.randomUUID()
            rapid.sendTestMessage(
                vedtakOpprettet(
                    vedtaksperiodeId = id,
                    fnr = "01010112345",
                    yrkesaktivitet = "SELVSTENDIG",
                    fom = LocalDate.of(2020, 1, 1),
                    tom = LocalDate.of(2020, 1, 31),
                    skjæringstidspunkt = LocalDate.of(2020, 1, 1),
                ),
            )

            assertFelt(
                id,
                forventetFnr = "01010112345",
                forventetAktørId = standardAktørId,
                forventetYrkesaktivitet = "SELVSTENDIG",
                forventetFom = LocalDate.of(2020, 1, 1),
                forventetTom = LocalDate.of(2020, 1, 31),
                forventetSkjæringstidspunkt = LocalDate.of(2020, 1, 1),
                forventetTilstand = "START",
            )
        }

    @Test
    fun `send samme periode igjen, men annen data`() =
        e2eTest {
            val id = UUID.randomUUID()
            rapid.sendTestMessage(
                vedtakOpprettet(
                    vedtaksperiodeId = id,
                    fnr = "01010112345",
                    yrkesaktivitet = "ARBEIDSTAKER",
                    orgnr = "et organisasjonsnummer",
                    fom = LocalDate.of(2020, 1, 1),
                    tom = LocalDate.of(2020, 1, 31),
                    skjæringstidspunkt = LocalDate.of(2020, 1, 1),
                ),
            )

            assertFelt(
                vedtaksperiodeId = id,
                forventetFnr = "01010112345",
                forventetAktørId = standardAktørId,
                forventetYrkesaktivitet = "et organisasjonsnummer",
                forventetFom = LocalDate.of(2020, 1, 1),
                forventetTom = LocalDate.of(2020, 1, 31),
                forventetSkjæringstidspunkt = LocalDate.of(2020, 1, 1),
                forventetTilstand = "START",
            )

            rapid.sendTestMessage(
                vedtakEndret(
                    vedtaksperiodeId = id,
                    fnr = "99999999999",
                    yrkesaktivitet = "kølgruvene",
                    fom = LocalDate.of(2029, 1, 1),
                    tom = LocalDate.of(2029, 1, 31),
                    skjæringstidspunkt = LocalDate.of(2029, 1, 1),
                    tilstand = "AVSLUTTET_UTEN_UTBETALING",
                ),
            )

            assertFelt(
                vedtaksperiodeId = id,
                forventetFnr = "99999999999",
                forventetAktørId = standardAktørId,
                forventetYrkesaktivitet = "kølgruvene",
                forventetFom = LocalDate.of(2029, 1, 1),
                forventetTom = LocalDate.of(2029, 1, 31),
                forventetSkjæringstidspunkt = LocalDate.of(2029, 1, 1),
                forventetTilstand = "AVSLUTTET_UTEN_UTBETALING",
            )
        }

    @Test
    fun `send person avstemt`() =
        e2eTest {
            val første = UUID.randomUUID()
            val andre = UUID.randomUUID()
            val tredje = UUID.randomUUID()
            rapid.sendTestMessage(personAvstemt(første, andre, tredje))
            assertFelt(
                første,
                forventetFnr = "1",
                forventetAktørId = standardAktørId,
                forventetYrkesaktivitet = "12",
                forventetSkjæringstidspunkt = LocalDate.of(2001, 1, 1),
                forventetTilstand = "START",
                forventetFom = LocalDate.of(2001, 1, 1),
                forventetTom = LocalDate.of(2001, 1, 11),
            )
            assertFelt(
                andre,
                forventetFnr = "1",
                forventetAktørId = standardAktørId,
                forventetYrkesaktivitet = "12",
                forventetSkjæringstidspunkt = LocalDate.of(2002, 2, 2),
                forventetTilstand = "AVSLUTTET_UTEN_UTBETALING",
                forventetFom = LocalDate.of(2002, 2, 2),
                forventetTom = LocalDate.of(2002, 2, 22),
            )
            assertFelt(
                tredje,
                forventetFnr = "1",
                forventetAktørId = standardAktørId,
                forventetYrkesaktivitet = "3",
                forventetSkjæringstidspunkt = LocalDate.of(2003, 3, 3),
                forventetTilstand = "AVSLUTTET",
                forventetFom = LocalDate.of(2003, 3, 3),
                forventetTom = LocalDate.of(2003, 3, 13),
            )
        }

    private fun vedtakOpprettet(
        vedtaksperiodeId: UUID,
        fnr: String,
        yrkesaktivitet: String,
        orgnr: String? = null,
        fom: LocalDate,
        tom: LocalDate,
        skjæringstidspunkt: LocalDate,
    ) = """
            {
            "@event_name": "vedtaksperiode_opprettet",
            "vedtaksperiodeId": "$vedtaksperiodeId",
            "fødselsnummer": "$fnr",
            "yrkesaktivitetstype": "$yrkesaktivitet",
            ${if (orgnr != null) "\"organisasjonsnummer\": \"$orgnr\"," else ""}
            "fom": "$fom",
            "tom": "$tom",
            "skjæringstidspunkt": "$skjæringstidspunkt"
            }
        """

    private fun vedtakEndret(
        vedtaksperiodeId: UUID,
        fnr: String,
        yrkesaktivitet: String,
        orgnr: String? = null,
        fom: LocalDate,
        tom: LocalDate,
        skjæringstidspunkt: LocalDate,
        tilstand: String,
    ) = """
            {
            "@event_name": "vedtaksperiode_endret",
            "vedtaksperiodeId": "$vedtaksperiodeId",
            "fødselsnummer": "$fnr",
            "yrkesaktivitetstype": "$yrkesaktivitet",
            ${if (orgnr != null) "\"organisasjonsnummer\": \"$orgnr\"," else ""}
            "fom": "$fom",
            "tom": "$tom",
            "skjæringstidspunkt": "$skjæringstidspunkt",
            "gjeldendeTilstand": "$tilstand"
            }
        """

    @Language("JSON")
    private fun personAvstemt(
        første: UUID,
        andre: UUID,
        tredje: UUID,
    ): String =
        """
  {
    "@event_name": "person_avstemt",
    "fødselsnummer": "1",
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

    private fun E2eTestApp.assertFelt(
        vedtaksperiodeId: UUID,
        forventetFnr: String,
        forventetAktørId: String,
        forventetYrkesaktivitet: String,
        forventetFom: LocalDate,
        forventetTom: LocalDate,
        forventetSkjæringstidspunkt: LocalDate,
        forventetTilstand: String,
    ) {
        val query = """select * from vedtaksperiode_data where vedtaksperiodeId = :vedtaksperiodeId"""
        sessionOf(dataSource).use { session ->
            val actuals: Map<String, Any> =
                session.run(
                    queryOf(query, mapOf("vedtaksperiodeId" to vedtaksperiodeId))
                        .map {
                            mapOf(
                                "fnr" to it.string("fnr"),
                                "aktørId" to it.string("aktorId"),
                                "yrkesaktivitet" to it.string("yrkesaktivitet"),
                                "fom" to it.localDate("fom"),
                                "tom" to it.localDate("tom"),
                                "tilstand" to it.string("tilstand"),
                                "skjæringstidspunkt" to it.localDate("skjaeringstidspunkt"),
                            )
                        }.asSingle,
                )!!

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
