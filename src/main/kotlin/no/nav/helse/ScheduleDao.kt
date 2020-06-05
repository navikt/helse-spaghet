package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import java.time.LocalDate
import javax.sql.DataSource

fun DataSource.settRapportert(dato: LocalDate) = using(sessionOf(this)) { session ->
    session.run(
        queryOf(
            """
                INSERT INTO schedule(melding_sendt)
                VALUES (?);
            """, dato
        )
            .asUpdate
    )
}

fun DataSource.erRapportert(dato: LocalDate) = requireNotNull(using(sessionOf(this)) { session ->
    session.run(
        queryOf(
            """
                SELECT count(*) > 0 AS er_sendt
                FROM schedule
                WHERE melding_sendt = ?
            """, dato
        )
            .map { it.boolean("er_sendt") }
            .asSingle
    )
})
