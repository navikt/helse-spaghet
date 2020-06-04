package no.nav.helse

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.ktor.client.HttpClient
import no.nav.helse.rapids_rivers.RapidApplication
import no.nav.helse.rapids_rivers.RapidsConnection
import java.time.LocalDate
import javax.sql.DataSource

val objectMapper = jacksonObjectMapper()
    .registerModule(JavaTimeModule())
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

suspend fun main() {
    val env = System.getenv()

    val dataSourceBuilder = DataSourceBuilder(env)
    val dataSource = dataSourceBuilder.getDataSource()
    val rapport = Rapport(dataSource.lagRapport(LocalDate.now()))
    val slackClient = SlackClient(
        HttpClient(),
        requireNotNull(env["SLACK_ACCESS_TOKEN"]) { "SLACK_ACCESS_TOKEN må settes" }
    )
    slackClient.postMessage(requireNotNull(env["RAPPORTERING_CHANNEL"]), rapport.tilMelding())

    RapidApplication.create(env)
        .setupRiver(dataSource)
        .setupMigration(dataSourceBuilder)
        .start()
}

fun <T: RapidsConnection> T.setupRiver(dataSource: DataSource) = apply {
    GodkjenningLøsning.Factory(this, dataSource)
}

private fun RapidsConnection.setupMigration(dataSourceBuilder: DataSourceBuilder) = apply {
    register(object : RapidsConnection.StatusListener {
        override fun onStartup(rapidsConnection: RapidsConnection) {
            dataSourceBuilder.migrate()
        }
    })
}
