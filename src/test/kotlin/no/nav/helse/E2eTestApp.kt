package no.nav.helse

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.github.navikt.tbd_libs.rapids_and_rivers.test_support.TestRapid
import com.github.navikt.tbd_libs.result_object.ok
import com.github.navikt.tbd_libs.spedisjon.HentMeldingResponse
import com.github.navikt.tbd_libs.spedisjon.SpedisjonClient
import com.github.navikt.tbd_libs.speed.IdentResponse
import com.github.navikt.tbd_libs.speed.SpeedClient
import com.github.navikt.tbd_libs.test_support.CleanupStrategy
import com.github.navikt.tbd_libs.test_support.DatabaseContainers
import com.github.navikt.tbd_libs.test_support.TestDataSource
import io.mockk.every
import io.mockk.mockk
import java.util.*
import java.time.LocalDateTime
import no.nav.helse.TestData.toJson
import no.nav.helse.TestData.toJsonUtenNotatTekst
import org.slf4j.LoggerFactory

private val tables = "annullering,annullering_arsak,begrunnelse,flyway_schema_history,funksjonell_feil,godkjenning,godkjenning_overstyringer,godkjenningsbehov,godkjenningsbehov_losning,godkjenningsbehov_losning_begrunnelse,hendelse_ikke_håndtert_årsak,oppgave,oppgave_endret,regelverksvarsel,revurdering,revurdering_vedtaksperiode,schedule,skatteinntekter_lagt_til_grunn,soknad,soknad_haandtert,utkast_til_vedtak,varsel,vedtaksperiode_aktivitet,vedtaksperiode_data,vedtaksperiode_tilstandsendring,vedtaksperiode_venter,warning_for_godkjenning,lagt_paa_vent"
private val databaseContainer = DatabaseContainers.container("spaghet", CleanupStrategy.tables(tables), walLevelLogical = true)

class E2eTestApp {
    var rapid: TestRapid = TestRapid()
        private set
    private var listAppender: ListAppender<ILoggingEvent> = ListAppender()
    private lateinit var testDataSource: TestDataSource
    val dataSource get() = testDataSource.ds

    val standardAktørId = "9999999999999"
    val speedClient = mockk<SpeedClient> {
        val innkommendefnr = mutableListOf<String>()
        every { hentFødselsnummerOgAktørId(capture(innkommendefnr), any()) } answers {
            IdentResponse(
                fødselsnummer = innkommendefnr.last(),
                aktørId = standardAktørId,
                npid = null,
                kilde = IdentResponse.KildeResponse.PDL
            ).ok()
        }
    }

    val spedisjonClient = mockk<SpedisjonClient> {
        every { hentMelding(any(), any()) } answers {
            HentMeldingResponse(
                type = "denne_type_kan_være_hva_som_helst_fordi_spaghet_bare_tar_ekstern_dokument_id_uansett",
                fnr = "99999999999",
                internDokumentId = UUID.randomUUID(),
                eksternDokumentId = UUID.randomUUID(),
                rapportertDato = LocalDateTime.now(),
                duplikatkontroll = "test_duplikatkontroll",
                jsonBody = "{}"
            ).ok()
        }
    }

    private fun start() {
        mockLog()
        testDataSource = databaseContainer.nyTilkobling()
        rapid.setupRivers(dataSource, speedClient, spedisjonClient)
    }

    fun LagtPåVent.sendTilRapid(medNotat: Boolean) {
        print(toJson())
        if (medNotat) rapid.sendTestMessage(toJson()) else rapid.sendTestMessage(toJsonUtenNotatTekst())
    }

    fun Annullering.sendTilRapid() {
        rapid.sendTestMessage(toJson())
    }

    fun TestData.VedtaksperiodeEndret.sendTilRapid() {
        rapid.sendTestMessage(toJson())
    }

    fun TestData.NyAktivitet.sendTilRapid() {
        rapid.sendTestMessage(toJson())
    }

    fun TestData.NyOppgave.sendTilRapid() {
        rapid.sendTestMessage(toJson())
    }

    private fun mockLog() {
        val logger = LoggerFactory.getLogger("spaghet") as Logger
        listAppender.start()
        logger.addAppender(listAppender)
    }

    private fun reset() {
        databaseContainer.droppTilkobling(testDataSource)
        rapid = TestRapid()
        listAppender = ListAppender()
    }


    companion object {
        private val testEnv by lazy { E2eTestApp() }
        fun e2eTest(f: E2eTestApp.() -> Unit) {
            try {
                testEnv.start()
                f(testEnv)
            } finally {
                testEnv.reset()
            }
        }
    }
}
