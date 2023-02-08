package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.ktor.client.*
import io.ktor.client.engine.apache.*
import io.ktor.serialization.jackson.jackson
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import no.nav.helse.rapids_rivers.RapidApplication
import no.nav.helse.rapids_rivers.RapidsConnection
import org.apache.http.impl.conn.SystemDefaultRoutePlanner
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.net.ProxySelector
import java.time.Duration
import java.time.LocalDate
import javax.sql.DataSource

val objectMapper: ObjectMapper = jacksonObjectMapper()
    .registerModule(JavaTimeModule())
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
val log: Logger = LoggerFactory.getLogger("spaghet")


suspend fun main() {
    val env = setUpEnvironment()

    val dataSourceBuilder = DataSourceBuilder(env.db)
    val dataSource = dataSourceBuilder.getDataSource()
//    val slackClient = SlackClient(
//        httpClient = HttpClient(Apache) {
//            engine {
//                customizeClient {
//                    setRoutePlanner(SystemDefaultRoutePlanner(ProxySelector.getDefault()))
//                }
//            }
//
//            install(io.ktor.client.plugins.contentnegotiation.ContentNegotiation) {
//                jackson()
//            }
//        },
//        accessToken = env.slack.accessToken
//    )
//
//    val channel = env.slack.raportChannel
//    GlobalScope.launch {
//        while (isActive) {
//            val iGår = LocalDate.now().minusDays(1)
//            log.info("er rapportert i går: ${dataSource.erRapportert(iGår)}")
//            if (!dataSource.erRapportert(iGår)) {
//                dataSource.settRapportert(iGår)
//                Rapport(dataSource.lagRapport(iGår), env.miljø).meldinger.forEach { melding ->
//                    val result = slackClient.postMessage(channel, melding.tekst)
//                    melding.tråd.forEach { trådmelding ->
//                        slackClient.postMessage(channel, trådmelding, result.ts)
//                    }
//                }
//            }
//            delay(Duration.ofMinutes(10L).toMillis())
//        }
//    }

    RapidApplication.create(env.raw)
        .setupRiver(dataSource)
        .setupMigration(dataSourceBuilder)
        .start()
}

fun <T : RapidsConnection> T.setupRiver(dataSource: DataSource) = apply {
    AnnulleringRiver(this, dataSource)
    GodkjenningLøsningRiver.Factory(this, dataSource)
    VedtaksperiodeTilGodkjenningRiver(this, dataSource)
    VedtaksperiodeBehandletRiver(this, dataSource)
    TidFraGodkjenningTilUtbetalingRiver(this, dataSource)
    TilstandendringRiver(this, dataSource)
    AktivitetRiver(this, dataSource)
    WarningsVedVedtakRiver(this, dataSource)
    HendelseIkkeHåndtertRiver(this, dataSource)
    RevurderingIgangsattRiver(this, dataSource)
    RevurderingFerdigstiltRiver(this, dataSource)
}

private fun RapidsConnection.setupMigration(dataSourceBuilder: DataSourceBuilder) = apply {
    register(object : RapidsConnection.StatusListener {
        override fun onStartup(rapidsConnection: RapidsConnection) {
            dataSourceBuilder.migrate()
        }
    })
}

fun JsonNode.valueOrNull(field: String): JsonNode? = takeIf { has(field) }?.get(field)
