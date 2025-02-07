package no.nav.helse.ventetilstand

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.DayOfWeek
import java.time.LocalDate

internal class VenteklassifiseringTest {

    @Test
    fun `gamle greier`() {
        val torsdag = torsdag(LocalDate.parse("2024-09-05"))
        assertEquals(Venteklassifisering.VANLIG, venteklassifisering(registrert = torsdag.minusDays(5).minusHours(23).minusMinutes(59).minusSeconds(59), torsdag))
        assertEquals(Venteklassifisering.GAMMEL, venteklassifisering(registrert = torsdag.minusDays(6), torsdag))
    }

    @Test
    fun `på hverdanger`() {
        val torsdag = torsdag(LocalDate.parse("2024-09-05"))
        assertEquals(Venteklassifisering.NYHET, venteklassifisering(registrert = torsdag.minusHours(23).minusMinutes(59).minusSeconds(59), torsdag))
        assertEquals(Venteklassifisering.NYHET, venteklassifisering(registrert = torsdag.minusHours(24), torsdag))
        assertEquals(Venteklassifisering.VANLIG, venteklassifisering(registrert = torsdag.minusHours(24).minusSeconds(1), torsdag))
    }

    @Test
    fun `på mandager`() {
        val fredag = fredag(LocalDate.parse("2024-08-30"))
        val lørdag = lørdag(LocalDate.parse("2024-08-31"))
        val søndag = søndag(LocalDate.parse("2024-09-01"))
        val mandag = mandag(LocalDate.parse("2024-09-02"))

        assertEquals(Venteklassifisering.VANLIG, venteklassifisering(registrert = fredag.minusSeconds(1), mandag))
        assertEquals(Venteklassifisering.NYHET, venteklassifisering(registrert = fredag, mandag))
        assertEquals(Venteklassifisering.NYHET, venteklassifisering(registrert = lørdag, mandag))
        assertEquals(Venteklassifisering.NYHET, venteklassifisering(registrert = søndag, mandag))
        assertEquals(Venteklassifisering.NYHET, venteklassifisering(registrert = mandag, mandag))
    }

    private fun mandag(dato: LocalDate) = dato.atStartOfDay().also { check(it.dayOfWeek == DayOfWeek.MONDAY) }
    private fun torsdag(dato: LocalDate) = dato.atStartOfDay().also { check(it.dayOfWeek == DayOfWeek.THURSDAY) }
    private fun fredag(dato: LocalDate) = dato.atStartOfDay().also { check(it.dayOfWeek == DayOfWeek.FRIDAY) }
    private fun lørdag(dato: LocalDate) = dato.atStartOfDay().also { check(it.dayOfWeek == DayOfWeek.SATURDAY) }
    private fun søndag(dato: LocalDate) = dato.atStartOfDay().also { check(it.dayOfWeek == DayOfWeek.SUNDAY) }
}
