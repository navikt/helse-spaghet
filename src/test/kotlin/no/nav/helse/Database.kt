package no.nav.helse

import com.opentable.db.postgres.embedded.EmbeddedPostgres
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.flywaydb.core.Flyway
import java.util.UUID
import javax.sql.DataSource

fun embeddedPostgres() = EmbeddedPostgres.builder().start()

internal fun setupDataSourceMedFlyway(embeddedPostgres: EmbeddedPostgres): DataSource {
    val hikariConfig = HikariConfig().apply {
        this.jdbcUrl = embeddedPostgres.getJdbcUrl("postgres", "postgres")
        maximumPoolSize = 3
        minimumIdle = 1
        idleTimeout = 10001
        connectionTimeout = 1000
        maxLifetime = 30001
    }

    val dataSource = HikariDataSource(hikariConfig)

    Flyway.configure()
        .dataSource(dataSource)
        .placeholders(mapOf("spesialist_oid" to UUID.randomUUID().toString()))
        .load()
        .migrate()

    return dataSource
}
