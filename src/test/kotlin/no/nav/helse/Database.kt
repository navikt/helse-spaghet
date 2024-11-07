package no.nav.helse

import kotliquery.Row
import kotliquery.queryOf
import no.nav.helse.Util.withSession
import java.util.*
import javax.sql.DataSource

fun DataSource.annulleringer(): List<Annullering> {
    return this.withSession {
        this.run(
            queryOf(
                """
SELECT a.saksbehandler, a.id, a.begrunnelser, a.kommentar, a.opprettet
FROM annullering a
ORDER BY a.opprettet DESC
    """
            )
                .map {
                    val årsaker = this.run(
                        queryOf("""
                            SELECT arsak, key
                            FROM annullering_arsak
                            WHERE vedtaksperiode_id = :annulleringId
                        """, mapOf(
                            "annulleringId" to it.string("id")
                        )).map {årsak -> årsak.annulleringÅrsak() }.asList)

                    it.annullering(årsaker)
                }.asList
        )
    }
}

fun Row.annullering(årsaker: List<AnnulleringArsak>?) =
    Annullering(
        saksbehandler = uuid("saksbehandler"),
        vedtaksperiodeId = UUID.fromString(string("id")),
        begrunnelser = årsaker?.takeUnless { it.isEmpty() }?.let { it.map { årsak -> årsak.arsak } } ?: stringList("begrunnelser"),
        kommentar = stringOrNull("kommentar"),
        opprettet = localDateTime("opprettet"),
        arsaker = årsaker?.takeUnless { it.isEmpty() } ?: emptyList()
    )

fun Row.annulleringÅrsak(): AnnulleringArsak =
    AnnulleringArsak(
        key = string("key"),
        arsak = string("arsak")
    )

fun Row.stringList(column: String) =
    objectMapper.readTree(string(column))
        .map { it.asText()!! }

