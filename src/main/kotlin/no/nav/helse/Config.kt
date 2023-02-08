package no.nav.helse

fun setUpEnvironment() = Environment(
    raw = System.getenv(),
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
