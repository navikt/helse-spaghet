package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.E2eTestApp.Companion.e2eTest
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.*

class HendelseIkkeHåndtertE2ETest {
    @Test
    fun `lagrer årsaker i hendelse_ikke_håndtert i database`() =
        e2eTest {
            val hendelseId = UUID.randomUUID()
            val årsaker =
                listOf(
                    "Mottatt flere søknader for perioden - det støttes ikke før replay av hendelser er på plass",
                    "pluss noe mer",
                )
            rapid.sendTestMessage(hendelseIkkeHåndtert(hendelseId, årsaker))
            assertEquals(
                listOf(
                    "Mottatt flere søknader for perioden - det støttes ikke før replay av hendelser er på plass",
                    "pluss noe mer",
                ),
                hentÅrsaker(hendelseId),
            )
        }

    @Test
    fun `uniqness på kombinasjoner av hendleseId, årsak`() =
        e2eTest {
            val hendelseId = UUID.randomUUID()
            val årsaker =
                listOf(
                    "Mottatt flere søknader for perioden - det støttes ikke før replay av hendelser er på plass",
                    "pluss noe mer",
                )
            rapid.sendTestMessage(hendelseIkkeHåndtert(hendelseId, årsaker))
            rapid.sendTestMessage(hendelseIkkeHåndtert(hendelseId, årsaker))
            assertEquals(
                listOf(
                    "Mottatt flere søknader for perioden - det støttes ikke før replay av hendelser er på plass",
                    "pluss noe mer",
                ),
                hentÅrsaker(hendelseId),
            )
        }

    private fun E2eTestApp.hentÅrsaker(hendelseId: UUID): List<String> =
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query =
                """
                SELECT * FROM hendelse_ikke_håndtert_årsak
                 WHERE hendelse_id = :hendelseId
                ;
                """.trimIndent()
            return session.run(
                queryOf(query, mapOf("hendelseId" to hendelseId))
                    .map { it.string("årsak") }
                    .asList,
            )
        }

    @Language("JSON")
    fun hendelseIkkeHåndtert(
        hendelseId: UUID,
        årsaker: List<String>,
    ) = """{
            "@event_name": "hendelse_ikke_håndtert",
            "hendelseId": "$hendelseId",
            "årsaker": ${årsaker.map { "\"${it}\"" }},
            "@opprettet": "2020-10-11T12:00:00.000000"
        }"""
}
