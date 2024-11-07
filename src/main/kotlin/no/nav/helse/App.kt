package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.navikt.tbd_libs.azure.AzureToken
import com.github.navikt.tbd_libs.azure.AzureTokenProvider
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import com.github.navikt.tbd_libs.result_object.Result
import com.github.navikt.tbd_libs.spurtedu.SpurteDuClient
import io.micrometer.core.instrument.Clock
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import io.prometheus.metrics.model.registry.PrometheusRegistry
import no.nav.helse.rapids_rivers.RapidApplication
import no.nav.helse.ventetilstand.*
import no.nav.helse.ventetilstand.IdentifiserStuckVedtaksperioder
import no.nav.helse.ventetilstand.VedtaksperiodeEndretRiver
import no.nav.helse.ventetilstand.VedtaksperiodeVenterIkkeRiver
import no.nav.helse.ventetilstand.VedtaksperiodeVenterRiver
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import javax.sql.DataSource

internal val objectMapper: ObjectMapper = jacksonObjectMapper()
    .registerModule(JavaTimeModule())
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
internal val logg: Logger = LoggerFactory.getLogger("spaghet")
internal val sikkerlogg: Logger = LoggerFactory.getLogger("tjenestekall")

private fun spurteDuClient() =
    SpurteDuClient(
        objectMapper = objectMapper,
        // har ikke behov for token provider fordi spaghet kun skjuler ting, og henter det ikke frem
        tokenProvider = object : AzureTokenProvider {
            override fun bearerToken(scope: String): Result<AzureToken> {
                TODO("Not yet implemented")
            }

            override fun onBehalfOfToken(scope: String, token: String): Result<AzureToken> {
                TODO("Not yet implemented")
            }
        }
    )

val meterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT, PrometheusRegistry.defaultRegistry, Clock.SYSTEM)

fun main() {
    val env = setUpEnvironment()

    val dataSourceBuilder = DataSourceBuilder(env.db)
    val dataSource = dataSourceBuilder.getDataSource()

    RapidApplication.create(env.raw, meterRegistry = meterRegistry)
        .setupRivers(dataSource)
        .setupMigration(dataSourceBuilder)
        .start()
}

internal fun <T : RapidsConnection> T.setupRivers(
    dataSource: DataSource,
    vedtaksperiodeVentetilstandDao: VedtaksperiodeVentetilstandDao = VedtaksperiodeVentetilstandDao(dataSource),
    oppsummeringDao: OppsummeringDao = OppsummeringDao(dataSource),
    spurteDuClient: SpurteDuClient = spurteDuClient()
) = apply {
    AnnulleringRiver(this, dataSource)
    GodkjenningLøsningRiver(this, dataSource)
    VedtaksperiodeTilGodkjenningRiver(this, dataSource)
    VedtaksperiodeBehandletRiver(this, dataSource)
    TidFraGodkjenningTilUtbetalingRiver(this, dataSource)
    TilstandendringRiver(this, dataSource)
    AktivitetRiver(this, dataSource)
    VarselEndretRiver(this, dataSource)
    HendelseIkkeHåndtertRiver(this, dataSource)
    RevurderingIgangsattRiver(this, dataSource)
    RevurderingFerdigstiltRiver(this, dataSource)
    VedtaksperiodeVenterRiver(this, vedtaksperiodeVentetilstandDao)
    VedtaksperiodeVenterIkkeRiver(this, vedtaksperiodeVentetilstandDao)
    VedtaksperiodeEndretRiver(this, vedtaksperiodeVentetilstandDao)
    IdentifiserStuckVedtaksperioder(this, vedtaksperiodeVentetilstandDao, spurteDuClient)
    OppsummerVedtaksperiodeVenter(this, oppsummeringDao)
    OppsummerVedtaksperiodeVenterExternal(this, oppsummeringDao)
    SøknadHåndtertRiver(this, dataSource)
    FunksjonellFeilOgVarselRiver(this, dataSource)
    SendtSøknadRiver(this, dataSource)
    OppgaveEndretRiver(this, dataSource)
    VedtaksperiodeEndretRiver(this, dataSource)
    VedtaksperiodeOpprettetRiver(this, dataSource)
    VedtaksperiodeAvstemt(this, dataSource)
    SkatteinntekterLagtTilGrunnRiver(this, dataSource)
    UtkastTilVedtakRiver(this, dataSource)
}

private fun RapidsConnection.setupMigration(dataSourceBuilder: DataSourceBuilder) = apply {
    register(object : RapidsConnection.StatusListener {
        override fun onStartup(rapidsConnection: RapidsConnection) {
            dataSourceBuilder.migrate()
        }
    })
}

fun JsonNode.valueOrNull(field: String): JsonNode? = takeIf { has(field) }?.get(field)
