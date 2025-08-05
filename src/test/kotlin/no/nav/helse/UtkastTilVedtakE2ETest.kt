package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.E2eTestApp.Companion.e2eTest
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.util.*

class UtkastTilVedtakE2ETest {
    @Test
    fun `lagrer i databasen`() =
        e2eTest {
            rapid.sendTestMessage(utkastTilVedtakEvent())
            assertEquals(1, tellUtkastTilVedtak())
        }

    @Test
    fun `lagrer bare unike kombinasjoner av (behandlingId, tags) databasen`() =
        e2eTest {
            val behandlingId = UUID.randomUUID()
            rapid.sendTestMessage(utkastTilVedtakEvent(behandlingId = behandlingId, tags = listOf("EnTag")))
            rapid.sendTestMessage(utkastTilVedtakEvent(behandlingId = behandlingId, tags = listOf("EnTag")))
            assertEquals(1, tellUtkastTilVedtak())
            rapid.sendTestMessage(utkastTilVedtakEvent(behandlingId = behandlingId, tags = listOf("EnTag", "EnTagTil")))
            assertEquals(2, tellUtkastTilVedtak())
        }

    private fun E2eTestApp.tellUtkastTilVedtak(): Int =
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT COUNT(*) FROM utkast_til_vedtak"
            requireNotNull(
                session.run(queryOf(query).map { row -> row.int(1) }.asSingle),
            )
        }

    @Language("JSON")
    private fun utkastTilVedtakEvent(
        behandlingId: UUID = UUID.randomUUID(),
        tags: List<String> = listOf("Tag"),
    ): String =
        """
        {
            "@event_name": "utkast_til_vedtak",
            "@id": "${UUID.randomUUID()}",
            "@opprettet": "${LocalDateTime.now()}",
            "vedtaksperiodeId": "${UUID.randomUUID()}",
            "behandlingId": "$behandlingId",
            "skjæringstidspunkt": "2018-01-01",
            "sykepengegrunnlagsfakta": {
              "6G": 561804.0
            },
            "fødselsnummer": "2",
            "tags": ${tags.map { "\"$it\"" }}
        }
        """
}
