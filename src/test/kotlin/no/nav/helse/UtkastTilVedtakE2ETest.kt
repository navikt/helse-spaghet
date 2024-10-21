package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Tags
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.LocalDateTime
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class UtkastTilVedtakE2ETest {
    private val embeddedPostgres = embeddedPostgres()
    private val dataSource = setupDataSourceMedFlyway(embeddedPostgres)
    private val river = TestRapid()
        .setupRivers(dataSource)

    @AfterEach
    fun slettRader() {
        return sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "TRUNCATE TABLE utkast_til_vedtak"
            session.run(queryOf(query).asExecute)
        }
    }

    @Test
    fun `lagrer i databasen`() {
        river.sendTestMessage(utkastTilVedtakEvent())
        assertEquals(1, tellUtkastTilVedtak())
    }

    @Test
    fun `lagrer bare unike kombinasjoner av (behandlingId, tags) databasen`() {
        val behandlingId = UUID.randomUUID()
        river.sendTestMessage(utkastTilVedtakEvent(behandlingId = behandlingId, tags = listOf("EnTag")))
        river.sendTestMessage(utkastTilVedtakEvent(behandlingId = behandlingId, tags = listOf("EnTag")))
        assertEquals(1, tellUtkastTilVedtak())
        river.sendTestMessage(utkastTilVedtakEvent(behandlingId = behandlingId, tags = listOf("EnTag", "EnTagTil")))
        assertEquals(2, tellUtkastTilVedtak())
    }

    private fun tellUtkastTilVedtak(): Int {
        return sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT COUNT(*) FROM utkast_til_vedtak"
            requireNotNull(
                session.run(queryOf(query).map { row -> row.int(1) }.asSingle)
            )
        }
    }

    @Language("JSON")
    private fun utkastTilVedtakEvent(behandlingId: UUID = UUID.randomUUID(), tags: List<String> = listOf("Tag")): String {
        return """
        {
            "@event_name": "utkast_til_vedtak",
            "@id": "${UUID.randomUUID()}",
            "@opprettet": "${LocalDateTime.now()}",
            "vedtaksperiodeId": "$behandlingId",
            "behandlingId": "${UUID.randomUUID()}",
            "skjæringstidspunkt": "2018-01-01",
            "sykepengegrunnlagsfakta": {
              "6G": 561804.0
            },
            "aktørId": "42",
            "fødselsnummer": "2",
            "tags": ${tags.joinToString(prefix = "[", postfix = "]")}
        }
        """

    }

}