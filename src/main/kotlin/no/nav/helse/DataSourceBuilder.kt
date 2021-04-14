package no.nav.helse

import com.zaxxer.hikari.HikariConfig
import org.flywaydb.core.Flyway
import javax.sql.DataSource
import no.nav.vault.jdbc.hikaricp.HikariCPVaultUtil.createHikariDataSourceWithVaultIntegration as createDataSource

internal class DataSourceBuilder(val env: Environment.DatabaseEnvironment) {
    private val hikariConfig = HikariConfig().apply {
        jdbcUrl = env.jdbcUrl
        maximumPoolSize = 3
        minimumIdle = 1
        idleTimeout = 10001
        connectionTimeout = 1000
        maxLifetime = 30001
    }

    fun getDataSource(role: Role = Role.User) =
        createDataSource(hikariConfig, env.vaultMountPath, role.asRole(env.databaseName))

    fun migrate() {
        runMigration(getDataSource(Role.Admin), """SET ROLE "${Role.Admin.asRole(env.databaseName)}"""")
    }

    private fun runMigration(dataSource: DataSource, initSql: String? = null) {
        Flyway.configure()
            .dataSource(dataSource)
            .initSql(initSql)
            .load()
            .migrate()
    }

    enum class Role {
        Admin, User, ReadOnly;

        fun asRole(databaseName: String) = "$databaseName-${name.toLowerCase()}"
    }
}
