package no.nav.helse

import com.zaxxer.hikari.HikariConfig
import org.flywaydb.core.Flyway
import javax.sql.DataSource
import no.nav.vault.jdbc.hikaricp.HikariCPVaultUtil.createHikariDataSourceWithVaultIntegration as createDataSource

internal class DataSourceBuilder(env: Map<String, String>) {
    private val databaseName = requireNotNull(env["DATABASE_NAME"]) { "database name must be set" }
    private val databaseHost = requireNotNull(env["DATABASE_HOST"]) { "database host must be set" }
    private val databasePort = requireNotNull(env["DATABASE_PORT"]) { "database port must be set" }
    private val vaultMountPath = requireNotNull(env["VAULT_MOUNTPATH"]) { "vault mount path must be set" }


    private val hikariConfig = HikariConfig().apply {
        jdbcUrl = "jdbc:postgresql://$databaseHost:$databasePort/$databaseName"
        maximumPoolSize = 3
        minimumIdle = 1
        idleTimeout = 10001
        connectionTimeout = 1000
        maxLifetime = 30001
    }

    fun getDataSource(role: Role = Role.User) =
        createDataSource(hikariConfig, vaultMountPath, role.asRole(databaseName))

    fun migrate() {
        runMigration(getDataSource(Role.Admin), """SET ROLE "${Role.Admin.asRole(databaseName)}"""")
    }

    private fun runMigration(dataSource: DataSource, initSql: String? = null): Int {
        return Flyway.configure()
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
