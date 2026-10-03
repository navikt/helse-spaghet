package no.nav.helse

import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import kotliquery.Session
import kotliquery.queryOf
import no.nav.helse.Util.asNullableText
import no.nav.helse.Util.asUuid
import no.nav.helse.Util.toJson
import org.intellij.lang.annotations.Language
import tools.jackson.databind.JsonNode
import java.time.LocalDateTime
import java.util.*

data class Annullering(
    val saksbehandler: UUID,
    val vedtaksperiodeId: UUID,
    val begrunnelser: List<String>,
    val kommentar: String?,
    val opprettet: LocalDateTime,
    val arsaker: List<AnnulleringArsak>?,
) {
    companion object {
        fun JsonNode.parseAnnullering(): Annullering =
            Annullering(
                saksbehandler = this["saksbehandler"]["oid"].asUuid(),
                vedtaksperiodeId = this["vedtaksperiodeId"].asUuid(),
                begrunnelser =
                    this["arsaker"]?.takeUnless { it.isEmpty }?.let { it.values().map { arsak -> arsak["arsak"].asString() } }
                        ?: this["begrunnelser"].values().map { it.asString() },
                kommentar = this["kommentar"].asNullableText(),
                opprettet = this["@opprettet"].asLocalDateTime(),
                arsaker =
                    this["arsaker"]?.values()?.map {
                        AnnulleringArsak(
                            key = it["key"].asString(),
                            arsak = it["arsak"].asString(),
                        )
                    } ?: emptyList(),
            )

        fun Session.insertAnnullering(annullering: Annullering) {
            @Language("PostgreSQL")
            val statement = """
                INSERT INTO annullering(
                    saksbehandler,
                    id,
                    id_type,
                    begrunnelser,
                    kommentar,
                    opprettet
                ) VALUES (?, ?, ?::id_type, ?, ?, ?)
                ON CONFLICT DO NOTHING;
            """
            run(
                queryOf(
                    statement,
                    annullering.saksbehandler,
                    annullering.vedtaksperiodeId,
                    "VEDTAKSPERIODE_ID",
                    annullering.begrunnelser.toJson(),
                    annullering.kommentar,
                    annullering.opprettet,
                ).asUpdate,
            )

            if (annullering.arsaker?.isNotEmpty() == true) {
                @Language("PostgreSQL")
                val årsakStatement = """
                INSERT INTO annullering_arsak(
                    arsak,
                    key,
                    vedtaksperiode_id
                ) VALUES (?, ?, ?);
            """
                annullering.arsaker.forEach { arsak ->
                    run(
                        queryOf(
                            årsakStatement,
                            arsak.arsak,
                            arsak.key,
                            annullering.vedtaksperiodeId,
                        ).asUpdate,
                    )
                }
            }
        }
    }
}

data class AnnulleringArsak(
    val key: String,
    val arsak: String,
)
