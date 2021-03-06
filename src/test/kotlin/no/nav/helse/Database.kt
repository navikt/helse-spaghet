package no.nav.helse

import com.opentable.db.postgres.embedded.EmbeddedPostgres
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotliquery.Row
import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.Util.uuid
import no.nav.helse.Util.withSession
import org.flywaydb.core.Flyway
import org.intellij.lang.annotations.Language
import java.util.*
import javax.sql.DataSource

fun embeddedPostgres() = EmbeddedPostgres.builder().start()

fun DataSource.annulleringer(): List<Annullering> {
    return this.withSession {
        this.run(
            queryOf(
                """
SELECT saksbehandler, fagsystem_id, begrunnelser, kommentar, opprettet
FROM annullering a
ORDER BY a.opprettet DESC
    """
            )
                .map { it.annullering() }.asList
        )
    }
}

fun Row.annullering() =
    Annullering(
        saksbehandler = uuid("saksbehandler"),
        fagsystemId = string("fagsystem_id"),
        begrunnelser = stringList("begrunnelser"),
        kommentar = stringOrNull("kommentar"),
        opprettet = localDateTime("opprettet"),
    )

fun Row.stringList(column: String) =
    objectMapper.readTree(string(column))
        .map { it.asText()!! }

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

    dataSource.createTruncateFunction()
    return dataSource
}

private fun DataSource.createTruncateFunction() {
    sessionOf(this).use {
        @Language("PostgreSQL")
        val query = """
            CREATE OR REPLACE FUNCTION truncate_tables() RETURNS void AS $$
            DECLARE
            truncate_statement text;
            BEGIN
                SELECT 'TRUNCATE ' || string_agg(format('%I.%I', schemaname, tablename), ',') || ' CASCADE'
                    INTO truncate_statement
                FROM pg_tables
                WHERE schemaname='public'
                AND tablename not in ('flyway_schema_history');
                EXECUTE truncate_statement;
            END;
            $$ LANGUAGE plpgsql;
        """
        it.run(queryOf(query).asExecute)
    }

}

fun DataSource.resetDatabase() {
    sessionOf(this).use {
        it.run(queryOf("SELECT truncate_tables()").asExecute)
    }
}
