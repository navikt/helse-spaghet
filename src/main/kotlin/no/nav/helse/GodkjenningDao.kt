package no.nav.helse

import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*
import javax.sql.DataSource
import kotliquery.Row
import kotliquery.Session
import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using

fun DataSource.insertGodkjenning(løsning: GodkjenningLøsningRiver) =
    using(sessionOf(this, returnGeneratedKey = true)) { session ->
        session.transaction { transaction ->
            val godkjenningId = transaction.run(
                queryOf(
                    """
                        INSERT INTO godkjenning(
                            vedtaksperiode_id,
                            aktor_id,
                            fodselsnummer,
                            godkjent_av,
                            godkjent_tidspunkt,
                            godkjent,
                            arsak,
                            kommentar,
                            periodetype,
                            inntektskilde
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    løsning.vedtaksperiodeId,
                    løsning.aktørId,
                    løsning.fødselsnummer,
                    løsning.godkjenning.saksbehandlerIdent,
                    løsning.godkjenning.godkjentTidspunkt,
                    løsning.godkjenning.godkjent,
                    løsning.godkjenning.årsak,
                    løsning.godkjenning.kommentar,
                    løsning.periodetype,
                    løsning.inntektskilde
                ).asUpdateAndReturnGeneratedKey
            )
            løsning.godkjenning.begrunnelser?.forEach { begrunnelse ->
                transaction.run(
                    queryOf("INSERT INTO begrunnelse(godkjenning_ref, tekst) VALUES(?, ?);", godkjenningId, begrunnelse)
                        .asUpdate
                )
            }
        }
    }

fun DataSource.lagRapport(dato: LocalDate = LocalDate.now()): List<GodkjenningDto> = using(sessionOf(this)) { session ->
    session.run(
        queryOf(
            """
SELECT vedtaksperiode_id,
       aktor_id,
       fodselsnummer,
       godkjent_tidspunkt,
       godkjent,
       arsak,
       kommentar,
       periodetype,
       json_agg(DISTINCT w.melding) AS warnings,
       json_agg(DISTINCT b.tekst) AS begrunnelser
FROM godkjenning g
         LEFT JOIN warning_for_godkjenning as w on g.id = w.godkjenning_ref
         LEFT JOIN begrunnelse b on g.id = b.godkjenning_ref
WHERE g.godkjent_tidspunkt::date = ?
GROUP BY g.id;
    """, dato
        )
            .map(::tilGodkjenningDto)
            .asList
    )
}

fun Session.findGodkjenningId(vedtaksperiodeId: UUID): Int? =
    this.run(
        queryOf(
            """
SELECT id
FROM godkjenning g
WHERE g.vedtaksperiode_id = ?
ORDER BY g.godkjent_tidspunkt DESC
LIMIT 1
    """, vedtaksperiodeId
        )
            .map { it.int("id") }.asSingle
    )

fun DataSource.godkjenningAlleredeLagret(vedtaksperiodeId: UUID, godkjentTidspunkt: LocalDateTime): Boolean =
    using(sessionOf(this)) { session ->
        session.run(
            queryOf(
                """
SELECT COUNT(1)
FROM godkjenning g
WHERE g.vedtaksperiode_id = ?
AND g.godkjent_tidspunkt = ?
""",
                vedtaksperiodeId, godkjentTidspunkt
            ).map { it.int(1) > 0 }.asSingle
        )!!
    }

data class GodkjenningDto(
    val vedtaksperiodeId: UUID,
    val aktørId: String,
    val fødselsnummer: String,
    val godkjentTidspunkt: LocalDateTime,
    val godkjent: Boolean,
    val årsak: String?,
    val kommentar: String?,
    val periodetype: String?,
    val warnings: List<String>,
    val begrunnelse: List<String>
)

fun tilGodkjenningDto(row: Row): GodkjenningDto = GodkjenningDto(
    vedtaksperiodeId = UUID.fromString(row.string("vedtaksperiode_id")),
    aktørId = row.string("aktor_id"),
    fødselsnummer = row.string("fodselsnummer"),
    godkjentTidspunkt = row.localDateTime("godkjent_tidspunkt"),
    godkjent = row.boolean("godkjent"),
    årsak = row.stringOrNull("arsak"),
    kommentar = row.stringOrNull("kommentar"),
    periodetype = row.stringOrNull("periodetype"),
    warnings = row.jsonArray("warnings"),
    begrunnelse = row.jsonArray("begrunnelser")
)

fun Row.jsonArray(column: String) = objectMapper.readTree(string(column))
    .filter { !it.isNull }
    .map { it.asText() }

