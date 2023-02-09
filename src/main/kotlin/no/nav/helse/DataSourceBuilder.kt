package no.nav.helse

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.flywaydb.core.Flyway
import javax.sql.DataSource

internal class DataSourceBuilder(private val env: Environment.DatabaseEnvironment) {
    private val hikariConfig = HikariConfig().apply {
        jdbcUrl = env.jdbcUrl
        maximumPoolSize = 3
        minimumIdle = 1
        idleTimeout = 10001
        connectionTimeout = 1000
        maxLifetime = 30001
        username = env.username
        password = env.password
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
