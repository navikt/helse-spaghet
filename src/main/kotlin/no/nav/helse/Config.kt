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
        username = System.getenv("DATABASE_USERNAME"),
        password = System.getenv("DATABASE_PASSWORD")
    )
)

class Environment(
    val raw: Map<String, String>,
    val slack: SlackEnvironment,
    val db: DatabaseEnvironment
) {
    val miljø = raw["NAIS_CLUSTER_NAME"]?.split("-")?.firstOrNull() ?: "ukjent miljø"

    data class SlackEnvironment(
        val accessToken: String,
        val raportChannel: String
    )

    class DatabaseEnvironment(
        val databaseName: String,
        val databaseHost: String,
        val databasePort: String,
        val username: String,
        val password: String,
    ) {
        val jdbcUrl = "jdbc:postgresql://$databaseHost:$databasePort/$databaseName"
    }
}
