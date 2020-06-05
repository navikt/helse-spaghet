package no.nav.helse

fun setUpEnvironment() = Environment(
    raw = System.getenv(),
    slack = Environment.SlackEnvironment(
        accessToken = System.getenv("SLACK_ACCESS_TOKEN"),
        raportChannel = System.getenv("RAPPORTERING_CHANNEL")
    ),
    db = Environment.DatabaseEnvironment(
        databaseName = System.getenv("DATABASE_NAME"),
        databaseHost = System.getenv("DATABASE_HOST"),
        databasePort = System.getenv("DATABASE_PORT"),
        vaultMountPath = System.getenv("VAULT_MOUNTPATH")
    )
)

class Environment(
    val raw: Map<String, String>,
    val slack: SlackEnvironment,
    val db: DatabaseEnvironment
) {
    data class SlackEnvironment(
        val accessToken: String,
        val raportChannel: String
    )

    class DatabaseEnvironment(
        val databaseName: String,
        val vaultMountPath: String,
        databaseHost: String,
        databasePort: String
    ) {
        val jdbcUrl = "jdbc:postgresql://$databaseHost:$databasePort/$databaseName"
    }
}
