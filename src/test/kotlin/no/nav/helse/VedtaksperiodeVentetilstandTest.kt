package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.E2eTestApp.Companion.e2eTest
import no.nav.helse.ventetilstand.VedtaksperiodeVenter
import no.nav.helse.ventetilstand.VedtaksperiodeVenter.Companion.vedtaksperiodeVenter
import no.nav.helse.ventetilstand.VedtaksperiodeVentetilstandDao
import no.nav.helse.ventetilstand.VenterPå
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*

internal class VedtaksperiodeVentetilstandTest {

    @Test
    fun `vedtaksperiode venter og går videre`() = e2eTest {
        val vedtaksperiodeId = UUID.randomUUID()
        val venterPåVedtaksperiodeId = UUID.randomUUID()
        assertNull(hentOmVenter(vedtaksperiodeId))
        rapid.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId))
        assertNotNull(hentOmVenter(vedtaksperiodeId))
        rapid.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId))
        assertNull(hentOmVenter(vedtaksperiodeId))
    }

    @Test
    fun `Gjentatte like vedtaksperiodeVenter lagres ikke, men så fort noe endres lagres det`() = e2eTest {
        val vedtaksperiodeId = UUID.randomUUID()
        val venterPåVedtaksperiodeId = UUID.randomUUID()
        rapid.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId))
        rapid.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId))
        rapid.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId))
        rapid.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId))
        rapid.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId))
        rapid.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId, "UTBETALING"))
        assertEquals("UTBETALING", hentOmVenter(vedtaksperiodeId)!!.venterPå.hva)
        rapid.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId))
        assertNull(hentOmVenter(vedtaksperiodeId))
    }

    @Test
    fun `Begynner å vente på nytt`() = e2eTest {
        val vedtaksperiodeId = UUID.randomUUID()
        val venterPåVedtaksperiodeId = UUID.randomUUID()
        assertNull(hentOmVenter(vedtaksperiodeId))
        rapid.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId))
        assertNotNull(hentOmVenter(vedtaksperiodeId))
        rapid.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId))
        assertNull(hentOmVenter(vedtaksperiodeId))
        rapid.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId))
        assertNotNull(hentOmVenter(vedtaksperiodeId))
    }

    @Test
    fun `lagrer riktig ting`() = e2eTest {
        val vedtaksperiodeId = UUID.fromString("00000000-0000-0000-0000-000000000000")
        val venterPåVedtaksperiodeId = UUID.fromString("00000000-0000-0000-0000-000000000001")
        rapid.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId, "TESTING"))
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
    fun `Ignorerer urelevante vedtaksperiode_endret`() = e2eTest {
        val vedtaksperiodeId = UUID.randomUUID()
        val venterPåVedtaksperiodeId = UUID.randomUUID()
        rapid.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId))
        assertNull(hentOmVenter(vedtaksperiodeId))
        rapid.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId))
        assertNotNull(hentOmVenter(vedtaksperiodeId))
        rapid.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId))
        assertNull(hentOmVenter(vedtaksperiodeId))
        rapid.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId))
        rapid.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId))
        rapid.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId))
        rapid.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId))
    }

    @Test
    fun `Hente ut siste ventetilstand på de som venter`() = e2eTest {
        val vedtaksperiodeId1 = UUID.randomUUID()
        val venterPåVedtaksperiodeId1 = UUID.randomUUID()

        val vedtaksperiodeId2 = UUID.randomUUID()
        val venterPåVedtaksperiodeId2 = UUID.randomUUID()

        val vedtaksperiodeId3 = UUID.randomUUID()
        val venterPåVedtaksperiodeId3 = UUID.randomUUID()

        rapid.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId1, venterPåVedtaksperiodeId1))
        rapid.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId2, venterPåVedtaksperiodeId2))
        rapid.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId3, venterPåVedtaksperiodeId3))

        assertEquals(setOf(vedtaksperiodeId1, vedtaksperiodeId2, vedtaksperiodeId3), hentVedtaksperiodeIderSomVenter())

        // vedtaksperiode1 fortsetter å vente med samme årsak
        rapid.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId1, venterPåVedtaksperiodeId1))
        // vedtaksperiode2 slutter å vente
        rapid.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId2))
        // vedtaksperiode 3 slutter å vente, og begynner å vente på noe annet
        rapid.sendTestMessage(vedtaksperiodeEndret(vedtaksperiodeId3))
        rapid.sendTestMessage(vedtaksperiodeVenter(vedtaksperiodeId3, venterPåVedtaksperiodeId3, "UTBETALING"))

        assertEquals(setOf(vedtaksperiodeId1, vedtaksperiodeId3), hentVedtaksperiodeIderSomVenter())
        val venteårsaker = hentDeSomVenter()
        assertEquals("GODKJENNING", venteårsaker.single { it.vedtaksperiodeId == vedtaksperiodeId1 }.venterPå.hva)
        assertEquals("UTBETALING", venteårsaker.single { it.vedtaksperiodeId == vedtaksperiodeId3 }.venterPå.hva)
    }

    @Test
    fun `venter ikke lengre når vi får eksplisitt signal på at vedtaksperiode ikke venter`() = e2eTest {
        assertEquals(emptySet<VedtaksperiodeVenter>(), hentDeSomVenter())
        val vedtaksperiodeId = UUID.randomUUID()
        val vedtaksperiodeVenter = vedtaksperiodeVenter(vedtaksperiodeId, UUID.randomUUID(), "HJELP", UUID.randomUUID())
        rapid.sendTestMessage(vedtaksperiodeVenter)
        assertEquals("HJELP", hentDeSomVenter().single().venterPå.hva)
        val vedtaksperiodeVenterIkke = vedtaksperiodeVenterIkke(vedtaksperiodeId)
        rapid.sendTestMessage(vedtaksperiodeVenterIkke)
        assertEquals(emptySet<VedtaksperiodeVenter>(), hentDeSomVenter())
    }

    @Test
    fun `vedtaksperiode som venter på HJELP skal defineres som stuck uavhengig av årsaken`() = e2eTest {
        assertEquals(emptyList<VedtaksperiodeVenter>(), stuck())
        val vedtaksperiodeId = UUID.randomUUID()
        val vedtaksperiodeVenter = vedtaksperiodeVenter(vedtaksperiodeId, UUID.randomUUID(), "HJELP", UUID.randomUUID(), venterPåHvorfor = "VIL_UTBETALES")
        rapid.sendTestMessage(vedtaksperiodeVenter)
        val stuck = stuck().single()
        assertEquals(vedtaksperiodeId, stuck.vedtaksperiodeId)
        assertEquals("HJELP", stuck.venterPå.hva)
        assertEquals("VIL_UTBETALES", stuck.venterPå.hvorfor)
    }

    @Test
    fun `ingen alarm når vi er stuck pga inntektsmelding UTEN hvorfor satt - vi får ikke gjort noe med dem uansett`() = e2eTest {
        assertEquals(emptyList<VedtaksperiodeVenter>(), stuck())
        val vedtaksperiodeId = UUID.randomUUID()
        val vedtaksperiodeVenter = vedtaksperiodeVenter(vedtaksperiodeId, UUID.randomUUID(), "INNTEKTSMELDING", UUID.randomUUID(), venterPåHvorfor = null)
        rapid.sendTestMessage(vedtaksperiodeVenter)
        assertEquals(emptyList<VedtaksperiodeVenter>(), stuck())
    }

    @Test
    fun `alarm når vi er stuck pga inntektsmelding MED hvorfor satt`() = e2eTest {
        assertEquals(emptyList<VedtaksperiodeVenter>(), stuck())
        val vedtaksperiodeId = UUID.randomUUID()
        val vedtaksperiodeVenter = vedtaksperiodeVenter(vedtaksperiodeId, UUID.randomUUID(), "INNTEKTSMELDING", UUID.randomUUID(), venterPåHvorfor = "noe")
        rapid.sendTestMessage(vedtaksperiodeVenter)
        val stuck = stuck().single()
        assertEquals(vedtaksperiodeId, stuck.vedtaksperiodeId)
        assertEquals("INNTEKTSMELDING", stuck.venterPå.hva)
        assertEquals("noe", stuck.venterPå.hvorfor)
    }

    @Test
    fun `alarm når hvorfor er satt til noe som helst utenom overstyring igangsatt`() = e2eTest {
        assertEquals(emptyList<VedtaksperiodeVenter>(), stuck())
        val vedtaksperiodeId = UUID.randomUUID()
        val vedtaksperiodeVenter = vedtaksperiodeVenter(vedtaksperiodeId, UUID.randomUUID(), "tja", UUID.randomUUID(), venterPåHvorfor = "tjo")
        rapid.sendTestMessage(vedtaksperiodeVenter)
        val stuck = stuck().single()
        assertEquals(vedtaksperiodeId, stuck.vedtaksperiodeId)
        assertEquals("tja", stuck.venterPå.hva)
        assertEquals("tjo", stuck.venterPå.hvorfor)
    }

    @Test
    fun `Ignorer meldinger med lik informasjon`() = e2eTest {
        val vedtaksperiodeId = UUID.randomUUID()
        val venterPåVedtaksperiodeId = UUID.randomUUID()
        val hendelseId1 = UUID.randomUUID()
        val hendelseId2 = UUID.randomUUID()
        val hendelseId3 = UUID.randomUUID()
        val vedtaksperiodeVenter1 = vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId, hendelseId = hendelseId1)
        val vedtaksperiodeVenter2 = vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId, hendelseId = hendelseId2)
        val vedtaksperiodeVenter3 = vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId, hendelseId = hendelseId3)
        rapid.sendTestMessage(vedtaksperiodeVenter1)
        rapid.sendTestMessage(vedtaksperiodeVenter2)
        rapid.sendTestMessage(vedtaksperiodeVenter3)
        assertEquals(hendelseId1, hentHendelseId(vedtaksperiodeId))

        val hendelseId4 = UUID.randomUUID()
        val vedtaksperiodeVenter4 = vedtaksperiodeVenter(vedtaksperiodeId, venterPåVedtaksperiodeId, hendelseId = hendelseId4, venterPåHvorfor = "OVERSTYRING_IGANGSATT")

        rapid.sendTestMessage(vedtaksperiodeVenter4)
        assertEquals(hendelseId4, hentHendelseId(vedtaksperiodeId))
    }

    @Language("JSON")
    private fun vedtaksperiodeVenter(
        vedtaksperiodeId: UUID,
        venterPåVedtaksperiodeId: UUID,
        venterPå: String = "GODKJENNING",
        hendelseId: UUID = UUID.randomUUID(),
        fødselsnummer: String = "11111111111",
        venterPåHvorfor: String? = "TESTOLINI",
        ventetSiden: LocalDateTime = LocalDateTime.parse("2023-03-04T21:34:17.96322")
    ) = """
        {
          "@event_name": "vedtaksperioder_venter",
          "@id": "$hendelseId",
          "fødselsnummer": "$fødselsnummer",
          "vedtaksperioder": [
            {
              "organisasjonsnummer": "123456789",
              "vedtaksperiodeId": "$vedtaksperiodeId",
              "skjæringstidspunkt": "2019-01-01",
              "ventetSiden": "$ventetSiden",
              "venterTil": "+999999999-12-31T23:59:59.999999999",
              "venterPå": {
                "vedtaksperiodeId": "$venterPåVedtaksperiodeId",
                "skjæringstidspunkt": "2018-01-01",
                "organisasjonsnummer": "987654321",
                "venteårsak": {
                  "hva": "$venterPå",
                  "hvorfor": ${venterPåHvorfor?.let { "\"$it\"" }}
                }
              }
            }
          ]
        }
    """

    @Language("JSON")
    private fun vedtaksperiodeEndret(vedtaksperiodeId: UUID, hendelseId: UUID = UUID.randomUUID(), fødselsnummer: String = "11111111111") = """
         {
          "@event_name": "vedtaksperiode_endret",
          "organisasjonsnummer": "123456789",
          "vedtaksperiodeId": "$vedtaksperiodeId",
          "gjeldendeTilstand": "AVVENTER_INNTEKTSMELDING",
          "forrigeTilstand": "AVVENTER_INFOTRYGDHISTORIKK",
          "@id": "$hendelseId",
          "fødselsnummer": "$fødselsnummer"
        } 
    """

    @Language("JSON")
    private fun vedtaksperiodeVenterIkke(vedtaksperiodeId: UUID, hendelseId: UUID = UUID.randomUUID()) = """
         {
          "@event_name": "vedtaksperiode_venter_ikke",
          "vedtaksperiodeId": "$vedtaksperiodeId",
          "@id": "$hendelseId"
        } 
    """

    private fun E2eTestApp.hentDeSomVenter() =
        sessionOf(dataSource).use { session ->
            session.list(queryOf("SELECT * FROM vedtaksperiode_venter")) { row ->
                row.vedtaksperiodeVenter
            }
        }.toSet()

    private fun E2eTestApp.hentVedtaksperiodeIderSomVenter() =
        hentDeSomVenter().map { it.vedtaksperiodeId }.toSet()

    private fun E2eTestApp.hentOmVenter(vedtaksperiodeId: UUID) =
        sessionOf(dataSource).use { session ->
            session.single(queryOf("SELECT * FROM vedtaksperiode_venter WHERE vedtaksperiodeId='$vedtaksperiodeId'")) { row ->
                row.vedtaksperiodeVenter
            }
        }

    private fun E2eTestApp.hentHendelseId(vedtaksperiodeId: UUID) =
        sessionOf(dataSource).use { session ->
            session.single(queryOf("SELECT hendelseId FROM vedtaksperiode_venter WHERE vedtaksperiodeId='$vedtaksperiodeId'")) { row ->
                row.uuid("hendelseId")
            }
        }

    private fun E2eTestApp.stuck(): List<VedtaksperiodeVenter> {
        // Hacker først tidsstempelet til å se ut til at de har ventet i 10 minutter
        sessionOf(dataSource).use { session ->
            session.execute(queryOf("UPDATE vedtaksperiode_venter SET tidsstempel=now() - INTERVAL '10 MINUTES'"))
        }
        return VedtaksperiodeVentetilstandDao(dataSource).stuck().map { it.vedtaksperiodeVenter }
    }
}