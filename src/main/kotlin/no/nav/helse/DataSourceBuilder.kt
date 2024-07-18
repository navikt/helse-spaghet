package no.nav.helse

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.flywaydb.core.Flyway
import java.time.Duration
import javax.sql.DataSource

internal class DataSourceBuilder(private val env: Environment.DatabaseEnvironment) {
    private val hikariConfig = HikariConfig().apply {
        jdbcUrl = env.jdbcUrl
        if (!env.erDev) {
            username = env.username
            password = env.password
        }

        maximumPoolSize = 5
        minimumIdle = 1

        connectionTimeout = Duration.ofSeconds(5).toMillis()
        maxLifetime = Duration.ofMinutes(30).toMillis()
        idleTimeout = Duration.ofMinutes(10).toMillis()
        initializationFailTimeout = Duration.ofMinutes(1).toMillis()
    }

    fun getDataSource() = HikariDataSource(hikariConfig)

    fun migrate() {
        getDataSource().use {
            runMigration(it)
        }
    }

    private fun runMigration(dataSource: DataSource) {
        Flyway.configure()
            .dataSource(dataSource)
            .lockRetryCount(-1)
            .load()
            .migrate()
    }
}
