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
    val fagsystemId: String,
    val begrunnelser: List<String>,
    val kommentar: String?,
    val gjelderSisteSkjæringstidspunkt: Boolean,
    val opprettet: LocalDateTime,
) {
    companion object {
        fun JsonNode.parseAnnullering(): Annullering {
            return Annullering(
                saksbehandler = this["saksbehandler"]["oid"].asUuid(),
                fagsystemId = this["fagsystemId"].asText(),
                begrunnelser = this["begrunnelser"].map { it.asText() },
                kommentar = this["kommentar"].asNullableText(),
                gjelderSisteSkjæringstidspunkt = this["gjelderSisteSkjæringstidspunkt"].asBoolean(),
                opprettet = this["@opprettet"].asLocalDateTime(),
            )
        }

        fun Session.insertAnnullering(annullering: Annullering) {
            @Language("PostgreSQL")
            val statement = """
                INSERT INTO annullering(
                    saksbehandler,
                    fagsystem_id,
                    begrunnelser,
                    kommentar,
                    gjelder_siste_skjæringstidspunkt,
                    opprettet
                ) VALUES ( ?, ?, ?, ?, ?, ? )
                ON CONFLICT DO NOTHING;
            """
            run(
                queryOf(
                    statement,
                    annullering.saksbehandler,
                    annullering.fagsystemId,
                    annullering.begrunnelser.toJson(),
                    annullering.kommentar,
                    annullering.gjelderSisteSkjæringstidspunkt,
                    annullering.opprettet,
                ).asUpdate
            )
        }
    }
}