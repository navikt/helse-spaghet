package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.E2eTestApp.Companion.e2eTest
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.util.*

class InnteksmeldingHåndtertE2ETest {
    @Test
    fun `lagrer i databasen`() =
        e2eTest {
            rapid.sendTestMessage(inntektsmeldingHåndertEvent())
            assertEquals(1, tellInntektsmeldingHåndtert())
        }

    private fun E2eTestApp.tellInntektsmeldingHåndtert(): Int =
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT COUNT(*) FROM inntektsmelding_haandtert"
            requireNotNull(
                session.run(queryOf(query).map { row -> row.int(1) }.asSingle),
            )
        }

    @Language("JSON")
    private fun inntektsmeldingHåndertEvent(
        vedtaksperiodeId: UUID = UUID.randomUUID(),
        hendelseId: UUID = UUID.randomUUID(),
    ): String =
        """
        {
            "@event_name": "inntektsmelding_håndtert",
            "@id": "${UUID.randomUUID()}",
            "@opprettet": "${LocalDateTime.now()}",
            "vedtaksperiodeId": "$vedtaksperiodeId",
            "inntektsmeldingId": "$hendelseId"
        }
        """
}
