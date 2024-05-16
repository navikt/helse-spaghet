package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import kotliquery.Session
import kotliquery.queryOf
import no.nav.helse.Util.asNullableText
import no.nav.helse.Util.asUuid
import no.nav.helse.Util.toJson
import no.nav.helse.rapids_rivers.asLocalDateTime
import org.intellij.lang.annotations.Language
import java.time.LocalDateTime
import java.util.*

data class Annullering(
    val saksbehandler: UUID,
    val vedtaksperiodeId: UUID,
    val begrunnelser: List<String>,
    val kommentar: String?,
    val opprettet: LocalDateTime,
) {
    companion object {
        fun JsonNode.parseAnnullering(): Annullering {
            return Annullering(
                saksbehandler = this["saksbehandler"]["oid"].asUuid(),
                vedtaksperiodeId = this["vedtaksperiodeId"].asUuid(),
                begrunnelser = this["begrunnelser"].map { it.asText() },
                kommentar = this["kommentar"].asNullableText(),
                opprettet = this["@opprettet"].asLocalDateTime(),
            )
        }

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
                ).asUpdate
            )
        }
    }
}
