package no.nav.helse

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.ktor.client.HttpClient
import io.ktor.client.engine.apache.Apache
import io.ktor.client.features.json.JacksonSerializer
import io.ktor.client.features.json.JsonFeature
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import no.nav.helse.rapids_rivers.RapidApplication
import no.nav.helse.rapids_rivers.RapidsConnection
import org.apache.http.impl.conn.SystemDefaultRoutePlanner
import org.slf4j.LoggerFactory
import java.net.ProxySelector
import java.time.Duration
import java.time.LocalDate
import javax.sql.DataSource

val objectMapper = jacksonObjectMapper()
    .registerModule(JavaTimeModule())
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
val log = LoggerFactory.getLogger("spaghet")


suspend fun main() {
    val env = System.getenv()

    val dataSourceBuilder = DataSourceBuilder(env)
    val dataSource = dataSourceBuilder.getDataSource()
    val slackClient = SlackClient(
        HttpClient(Apache) {
            engine {
                customizeClient {
                    setRoutePlanner(SystemDefaultRoutePlanner(ProxySelector.getDefault()))
                }
            }

            install(JsonFeature) {
                this.serializer = JacksonSerializer(objectMapper)
            }
        },
        requireNotNull(env["SLACK_ACCESS_TOKEN"]) { "SLACK_ACCESS_TOKEN må settes" }
    )
    val channel = requireNotNull(env["RAPPORTERING_CHANNEL"])

    GlobalScope.launch {
        while (isActive) {
            val iGår = LocalDate.now().minusDays(1)
            log.info("er rapportert i går: ${dataSource.erRapportert(iGår)}")
            if (!dataSource.erRapportert(iGår)) {
                dataSource.settRapportert(iGår)
                Rapport(dataSource.lagRapport(iGår)).meldinger.forEach { melding ->
                    val result = slackClient.postMessage(channel, melding.tekst)
                    melding.tråd.forEach { trådmelding ->
                        slackClient.postMessage(channel, trådmelding, result.ts)
                    }
                }
            }
            delay(Duration.ofMinutes(10L).toMillis())
        }
    }

    RapidApplication.create(env)
        .setupRiver(dataSource)
        .setupMigration(dataSourceBuilder)
        .start()
}

fun <T : RapidsConnection> T.setupRiver(dataSource: DataSource) = apply {
    GodkjenningLøsning.Factory(this, dataSource)
}

private fun RapidsConnection.setupMigration(dataSourceBuilder: DataSourceBuilder) = apply {
    register(object : RapidsConnection.StatusListener {
        override fun onStartup(rapidsConnection: RapidsConnection) {
            dataSourceBuilder.migrate()
        }
    })
}
