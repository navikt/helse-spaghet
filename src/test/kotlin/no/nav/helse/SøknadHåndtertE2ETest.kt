package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.E2eTestApp.Companion.e2eTest
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class SøknadHåndtertE2ETest {
    @Test
    fun `lagrer kobling mellom søknad og vedtaksperiode`() =
        e2eTest {
            rapid.sendTestMessage(søknadHåndtertEvent())
            assertEquals(1, tellSøknadHåndtert())
        }

    @Test
    fun `lagrer ikke duplikat kobling mellom søknad og vedtaksperiode`() =
        e2eTest {
            rapid.sendTestMessage(søknadHåndtertEvent())
            rapid.sendTestMessage(søknadHåndtertEvent())
            assertEquals(1, tellSøknadHåndtert())
        }

    private fun E2eTestApp.tellSøknadHåndtert(): Int =
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "SELECT COUNT(*) FROM soknad_haandtert"
            requireNotNull(
                session.run(queryOf(query).map { row -> row.int(1) }.asSingle),
            )
        }

    private fun søknadHåndtertEvent() =
        """
        {
          "@event_name": "søknad_håndtert",
          "søknadId": "e0653625-02ac-40ae-a74d-7bdf45c4a903",
          "vedtaksperiodeId": "fd24cf75-f4ab-4773-bfa9-ffb60e97e8a9",
          "@opprettet": "2023-05-05T11:27:05.525698943"
        }
    """
}
