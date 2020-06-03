package no.nav.helse

import no.nav.helse.rapids_rivers.RapidApplication
import no.nav.helse.rapids_rivers.RapidsConnection
import javax.sql.DataSource

fun main() {
    val env = System.getenv()

    val dataSourceBuilder = DataSourceBuilder(env)
    val dataSource = dataSourceBuilder.getDataSource()

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
