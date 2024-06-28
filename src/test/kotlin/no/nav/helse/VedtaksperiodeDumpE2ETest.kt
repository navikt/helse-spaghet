package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class VedtaksperiodeDumpE2ETest {
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
        river.sendTestMessage(melding(id, "01010112345", "12345", "et organisasjonsnummer", LocalDate.of(2020, 1, 1), LocalDate.of(2020, 1, 31), LocalDate.of(2020, 1, 1), "AVSLUTTET", LocalDateTime.of(2020, 1, 1, 1, 1, 1)))

        assertFelt(id,
            forventetFnr = "01010112345",
            forventetAktørId = "12345",
            forventetYrkesaktivitet = "et organisasjonsnummer",
            forventetFom = LocalDate.of(2020, 1, 1),
            forventetTom = LocalDate.of(2020, 1, 31),
            forventetSkjæringstidspunkt = LocalDate.of(2020, 1, 1),
            forventetTilstand = "AVSLUTTET",
            forventetOppdatert = LocalDateTime.of(2020, 1, 1, 1, 1, 1)
        )
    }

    @Test
    fun `send samme periode igjen, men annen data`() {
        val id = UUID.randomUUID()
        river.sendTestMessage(melding(id, "01010112345", "12345", "et organisasjonsnummer", LocalDate.of(2020, 1, 1), LocalDate.of(2020, 1, 31), LocalDate.of(2020, 1, 1), "AVSLUTTET", LocalDateTime.of(2020, 1, 1, 1, 1, 1)))
        river.sendTestMessage(melding(id, "99999999999", "99999", "kølgruvene", LocalDate.of(2029, 1, 1), LocalDate.of(2029, 1, 31), LocalDate.of(2029, 1, 1), "AVSLUTTET_UTEN_UTBETALING", LocalDateTime.of(2029, 1, 1, 1, 1, 1)))

        assertFelt(id,
            forventetFnr = "99999999999",
            forventetAktørId = "99999",
            forventetYrkesaktivitet = "kølgruvene",
            forventetFom = LocalDate.of(2029, 1, 1),
            forventetTom = LocalDate.of(2029, 1, 31),
            forventetSkjæringstidspunkt = LocalDate.of(2029, 1, 1),
            forventetTilstand = "AVSLUTTET_UTEN_UTBETALING",
            forventetOppdatert = LocalDateTime.of(2029, 1, 1, 1, 1, 1)
        )
    }

    private fun melding(vedtaksperiodeId: UUID, fnr: String, aktørId: String, yrkesaktivitet: String, fom: LocalDate, tom: LocalDate, skjæringstidspunkt: LocalDate, tilstand: String, oppdatert: LocalDateTime) = """
            {
            "@event_name": "vedtaksperiode_data",
            "vedtaksperiodeId": "$vedtaksperiodeId",
            "fødselsnummer": "$fnr",
            "aktørId": "$aktørId",
            "yrkesaktivitet": "$yrkesaktivitet",
            "fom": "$fom",
            "tom": "$tom",
            "skjæringstidspunkt": "$skjæringstidspunkt",
            "tilstand": "$tilstand",
            "oppdatert": "$oppdatert"
            }
        """

    private fun assertFelt(vedtaksperiodeId: UUID,
                           forventetFnr: String,
                           forventetAktørId: String,
                           forventetYrkesaktivitet: String,
                           forventetFom: LocalDate,
                           forventetTom: LocalDate,
                           forventetSkjæringstidspunkt: LocalDate,
                           forventetTilstand: String,
                           forventetOppdatert: LocalDateTime
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
                        "skjæringstidspunkt" to it.localDate("skjaeringstidspunkt"),
                        "oppdatert" to it.localDateTime("oppdatert"))
                }.asSingle)!!

            assertEquals(forventetFnr, actuals["fnr"]!!)
            assertEquals(forventetAktørId, actuals["aktørId"]!!)
            assertEquals(forventetYrkesaktivitet, actuals["yrkesaktivitet"]!!)
            assertEquals(forventetFom, actuals["fom"]!!)
            assertEquals(forventetTom, actuals["tom"]!!)
            assertEquals(forventetSkjæringstidspunkt, actuals["skjæringstidspunkt"]!!)
            assertEquals(forventetTilstand, actuals["tilstand"]!!)
            assertEquals(forventetOppdatert, actuals["oppdatert"]!!)
        }
    }
}
