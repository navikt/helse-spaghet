package no.nav.helse

import org.flywaydb.core.api.MigrationVersion
import org.flywaydb.core.api.MigrationVersion.LATEST
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.postgresql.util.PSQLException
import java.util.UUID

internal class VedtaksperiodeVentetilstandGjeldendeTest : AbstractVedtaksperiodeVentetilstandTest(
    configureDb = { setupFlyway(it, MigrationVersion.fromVersion("54")) }
) {

    @Test
    fun `hente vedtaksperioder som venter gir samme resultat ved å bruke migrert gjeldende flagg`() {
        val vedtaksperioder = setOf(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID())
        val venterPå = setOf("EN", "TO", "TRE")

        vedtaksperioder.forEachIndexed { fnr, vedtaksperiode ->
            venterPå.forEach {
                river.sendTestMessage(vedtaksperiodeVenter(fødselsnummer = "$fnr", venterPåVedtaksperiodeId = vedtaksperiode, vedtaksperiodeId = vedtaksperiode, venterPå = it))
            }
        }

        assertEquals(9, antallRader)

        vedtaksperioder.forEachIndexed { fnr, vedtaksperiode ->
            river.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId = vedtaksperiode, fødselsnummer = "$fnr"))
        }

        assertEquals(12, antallRader)

        vedtaksperioder.take(2).forEachIndexed { fnr, vedtaksperiode ->
            river.sendTestMessage(vedtaksperiodeVenter(fødselsnummer = "$fnr", venterPåVedtaksperiodeId = vedtaksperiode, vedtaksperiodeId = vedtaksperiode, venterPå = "FIRE"))
        }

        assertEquals(14, antallRader)

        val venterFørMigrering = hentDeSomVenterBasertPåTimestamp()
        assertThrows<PSQLException> { hentDeSomVenterBasertPåGjeldende() } // Migrering som legger til felt ikke kjørt enda

        flyway.target(LATEST).load().migrate()

        assertEquals(venterFørMigrering, hentDeSomVenterBasertPåTimestamp())
        assertEquals(venterFørMigrering, hentDeSomVenterBasertPåGjeldende())
        assertEquals(2, venterFørMigrering.size)
        assertEquals("FIRE", venterFørMigrering.single { it.fødselsnummer == "0" }.venterPå.hva)
        assertEquals("FIRE", venterFørMigrering.single { it.fødselsnummer == "1" }.venterPå.hva)

        // Også de som ikke venter er "gjeldende" 💡
        val forventetGjeldende = vedtaksperioder.mapIndexed { fnr, vedtaksperiode -> vedtaksperiode to "$fnr" }.toMap()

        assertEquals(forventetGjeldende, hentGjeldende())
    }
}