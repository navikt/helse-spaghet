package no.nav.helse

import no.nav.helse.ventetilstand.VedtaksperiodeVenter
import no.nav.helse.ventetilstand.VenterPå
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*

internal class VedtaksperiodeVentetilstandTest : AbstractVedtaksperiodeVentetilstandTest() {

    @Test
    fun `vedtaksperiode venter og går videre`() {
        val vedtaksperiodeId = UUID.randomUUID()
        val venterPåVedtaksperiodeId = UUID.randomUUID()
        assertNull(hentOmVenter(vedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId))
        assertNotNull(hentOmVenter(vedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId))
        assertNull(hentOmVenter(vedtaksperiodeId))
    }

    @Test
    fun `Gjentatte like vedtaksperiodeVenter lagres ikke, men så fort noe endres lagres det`() {
        val vedtaksperiodeId = UUID.randomUUID()
        val venterPåVedtaksperiodeId = UUID.randomUUID()
        river.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId, "UTBETALING"))
        assertEquals("UTBETALING", hentOmVenter(vedtaksperiodeId)!!.venterPå.hva)
        river.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId))
        assertNull(hentOmVenter(vedtaksperiodeId))
    }

    @Test
    fun `Begynner å vente på nytt`() {
        val vedtaksperiodeId = UUID.randomUUID()
        val venterPåVedtaksperiodeId = UUID.randomUUID()
        assertNull(hentOmVenter(vedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId))
        assertNotNull(hentOmVenter(vedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId))
        assertNull(hentOmVenter(vedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId))
        assertNotNull(hentOmVenter(vedtaksperiodeId))
    }

    @Test
    fun `lagrer riktig ting`() {
        val vedtaksperiodeId = UUID.fromString("00000000-0000-0000-0000-000000000000")
        val venterPåVedtaksperiodeId = UUID.fromString("00000000-0000-0000-0000-000000000001")
        river.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId, "TESTING"))
        val forventet = VedtaksperiodeVenter.opprett(
            vedtaksperiodeId = vedtaksperiodeId,
            skjæringstidspunkt = LocalDate.parse("2019-01-01"),
            fødselsnummer = "11111111111",
            organisasjonsnummer = "123456789",
            ventetSiden = LocalDateTime.parse("2023-03-04T21:34:17"),
            venterTil = LocalDateTime.parse("9999-12-31T23:59:59"),
            venterPå = VenterPå(
                vedtaksperiodeId = venterPåVedtaksperiodeId,
                skjæringstidspunkt = LocalDate.parse("2018-01-01"),
                organisasjonsnummer = "987654321",
                hva = "TESTING",
                hvorfor = "TESTOLINI"
            )
        )
        assertEquals(forventet, hentOmVenter(vedtaksperiodeId))
    }

    @Test
    fun `Ignorerer urelevante vedtaksperiode_endret`() {
        val vedtaksperiodeId = UUID.randomUUID()
        val venterPåVedtaksperiodeId = UUID.randomUUID()
        river.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId))
        assertNull(hentOmVenter(vedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId))
        assertNotNull(hentOmVenter(vedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId))
        assertNull(hentOmVenter(vedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId))
        river.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId))
    }

    @Test
    fun `Hente ut siste ventetilstand på de som venter`() {
        val vedtaksperiodeId1 = UUID.randomUUID()
        val venterPåVedtaksperiodeId1 = UUID.randomUUID()

        val vedtaksperiodeId2 = UUID.randomUUID()
        val venterPåVedtaksperiodeId2 = UUID.randomUUID()

        val vedtaksperiodeId3 = UUID.randomUUID()
        val venterPåVedtaksperiodeId3 = UUID.randomUUID()

        river.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId1, venterPåVedtaksperiodeId1))
        river.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId2, venterPåVedtaksperiodeId2))
        river.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId3, venterPåVedtaksperiodeId3))

        assertEquals(setOf(vedtaksperiodeId1, vedtaksperiodeId2, vedtaksperiodeId3), hentVedtaksperiodeIderSomVenter())

        // vedtaksperiode1 fortsetter å vente med samme årsak
        river.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId1, venterPåVedtaksperiodeId1))
        // vedtaksperiode2 slutter å vente
        river.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId2))
        // vedtaksperiode 3 slutter å vente, og begynner å vente på noe annet
        river.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId3))
        river.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId3, venterPåVedtaksperiodeId3, "UTBETALING"))

        assertEquals(setOf(vedtaksperiodeId1, vedtaksperiodeId3), hentVedtaksperiodeIderSomVenter())
        val venteårsaker = hentDeSomVenter()
        assertEquals("GODKJENNING", venteårsaker.single { it.vedtaksperiodeId == vedtaksperiodeId1 }.venterPå.hva)
        assertEquals("UTBETALING", venteårsaker.single { it.vedtaksperiodeId == vedtaksperiodeId3 }.venterPå.hva)
    }

    @Test
    fun `venter ikke lengre når vi får eksplisitt signal på at vedtaksperiode ikke venter`() {
        assertEquals(emptySet<VedtaksperiodeVenter>(), hentDeSomVenter())
        val vedtaksperiodeId = UUID.randomUUID()
        val vedtaksperiodeVenter = vedtaksperiodeVenter(vedtaksperiodeId, UUID.randomUUID(), "HJELP", UUID.randomUUID())
        river.sendTestMessage(vedtaksperiodeVenter)
        assertEquals("HJELP", hentDeSomVenter().single().venterPå.hva)
        val vedtaksperiodeVenterIkke = vedtaksperiodeVenterIkke(vedtaksperiodeId)
        river.sendTestMessage(vedtaksperiodeVenterIkke)
        assertEquals(emptySet<VedtaksperiodeVenter>(), hentDeSomVenter())
    }

    @Test
    fun `vedtaksperiode som venter på HJELP skal defineres som stuck uavhengig av årsaken`() {
        assertEquals(emptyList<VedtaksperiodeVenter>(), stuck())
        val vedtaksperiodeId = UUID.randomUUID()
        val vedtaksperiodeVenter = vedtaksperiodeVenter(vedtaksperiodeId, UUID.randomUUID(), "HJELP", UUID.randomUUID(), venterPåHvorfor = "VIL_UTBETALES")
        river.sendTestMessage(vedtaksperiodeVenter)
        val stuck = stuck().single()
        assertEquals(vedtaksperiodeId, stuck.vedtaksperiodeId)
        assertEquals("HJELP", stuck.venterPå.hva)
        assertEquals("VIL_UTBETALES", stuck.venterPå.hvorfor)
    }

    @Test
    fun `ingen alarm når vi er stuck pga inntektsmelding UTEN hvorfor satt - vi får ikke gjort noe med dem uansett`() {
        assertEquals(emptyList<VedtaksperiodeVenter>(), stuck())
        val vedtaksperiodeId = UUID.randomUUID()
        val vedtaksperiodeVenter = vedtaksperiodeVenter(vedtaksperiodeId, UUID.randomUUID(), "INNTEKTSMELDING", UUID.randomUUID(), venterPåHvorfor = null)
        river.sendTestMessage(vedtaksperiodeVenter)
        assertEquals(emptyList<VedtaksperiodeVenter>(), stuck())
    }

    @Test
    fun `alarm når vi er stuck pga inntektsmelding MED hvorfor satt`() {
        assertEquals(emptyList<VedtaksperiodeVenter>(), stuck())
        val vedtaksperiodeId = UUID.randomUUID()
        val vedtaksperiodeVenter = vedtaksperiodeVenter(vedtaksperiodeId, UUID.randomUUID(), "INNTEKTSMELDING", UUID.randomUUID(), venterPåHvorfor = "noe")
        river.sendTestMessage(vedtaksperiodeVenter)
        val stuck = stuck().single()
        assertEquals(vedtaksperiodeId, stuck.vedtaksperiodeId)
        assertEquals("INNTEKTSMELDING", stuck.venterPå.hva)
        assertEquals("noe", stuck.venterPå.hvorfor)
    }

    @Test
    fun `alarm når hvorfor er satt til noe som helst utenom overstyring igangsatt`() {
        assertEquals(emptyList<VedtaksperiodeVenter>(), stuck())
        val vedtaksperiodeId = UUID.randomUUID()
        val vedtaksperiodeVenter = vedtaksperiodeVenter(vedtaksperiodeId, UUID.randomUUID(), "tja", UUID.randomUUID(), venterPåHvorfor = "tjo")
        river.sendTestMessage(vedtaksperiodeVenter)
        val stuck = stuck().single()
        assertEquals(vedtaksperiodeId, stuck.vedtaksperiodeId)
        assertEquals("tja", stuck.venterPå.hva)
        assertEquals("tjo", stuck.venterPå.hvorfor)
    }

    @Test
    fun `Ignorer meldinger med lik informasjon`() {
        val vedtaksperiodeId = UUID.randomUUID()
        val venterPåVedtaksperiodeId = UUID.randomUUID()
        val hendelseId1 = UUID.randomUUID()
        val hendelseId2 = UUID.randomUUID()
        val hendelseId3 = UUID.randomUUID()
        val vedtaksperiodeVenter1 = vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId, hendelseId = hendelseId1)
        val vedtaksperiodeVenter2 = vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId, hendelseId = hendelseId2)
        val vedtaksperiodeVenter3 = vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId, hendelseId = hendelseId3)
        river.sendTestMessage(vedtaksperiodeVenter1)
        river.sendTestMessage(vedtaksperiodeVenter2)
        river.sendTestMessage(vedtaksperiodeVenter3)
        assertEquals(hendelseId1, hentHendelseId(vedtaksperiodeId))

        val hendelseId4 = UUID.randomUUID()
        val vedtaksperiodeVenter4 = vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId, hendelseId = hendelseId4, venterPåHvorfor = "OVERSTYRING_IGANGSATT")

        river.sendTestMessage(vedtaksperiodeVenter4)
        assertEquals(hendelseId4, hentHendelseId(vedtaksperiodeId))
    }
}