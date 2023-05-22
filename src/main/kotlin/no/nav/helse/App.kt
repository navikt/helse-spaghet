package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import no.nav.helse.rapids_rivers.RapidApplication
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.ventetilstand.VedtaksperiodeEndretRiver
import no.nav.helse.ventetilstand.VedtaksperiodeVenterRiver
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import javax.sql.DataSource

internal val objectMapper: ObjectMapper = jacksonObjectMapper()
    .registerModule(JavaTimeModule())
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
internal val logg: Logger = LoggerFactory.getLogger("spaghet")
internal val sikkerlogg: Logger = LoggerFactory.getLogger("tjenestekall")


fun main() {
    val env = setUpEnvironment()

    val dataSourceBuilder = DataSourceBuilder(env.db)
    val dataSource = dataSourceBuilder.getDataSource()

    RapidApplication.create(env.raw)
        .setupRivers(dataSource)
        .setupMigration(dataSourceBuilder)
        .start()
}

fun <T : RapidsConnection> T.setupRivers(dataSource: DataSource) = apply {
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
    OverlappendeInfotrygdperiodeEtterInfotrygdendringRiver(this, dataSource)
    VedtaksperiodeVenterRiver(this, dataSource)
    VedtaksperiodeEndretRiver(this, dataSource)
    SøknadHåndtertRiver(this, dataSource)
    FunksjonellFeilOgVarselRiver(this, dataSource)
    SendtSøknadRiver(this, dataSource)
}

private fun RapidsConnection.setupMigration(dataSourceBuilder: DataSourceBuilder) = apply {
    register(object : RapidsConnection.StatusListener {
        override fun onStartup(rapidsConnection: RapidsConnection) {
            dataSourceBuilder.migrate()
        }
    })
}

fun JsonNode.valueOrNull(field: String): JsonNode? = takeIf { has(field) }?.get(field)
