package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDate
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import kotliquery.Session
import kotliquery.queryOf
import no.nav.helse.Util.asUuid
import org.intellij.lang.annotations.Language
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.UUID

data class LagtPåVentÅrsak(
    val key: String,
    val årsak: String
)

data class LagtPåVent(
    val oppgaveId: Long,
    val behandlingId: UUID,
    val skalTildeles: Boolean,
    val frist: LocalDate,
    val opprettet: LocalDateTime,
    val saksbehandlerOid: UUID,
    val saksbehandlerIdent: String,
    val notatTekst: String?,
    val årsaker: List<LagtPåVentÅrsak>
) {
    companion object {
        fun JsonNode.parseLeggPåVent(): LagtPåVent {
            return LagtPåVent(
                oppgaveId = this["oppgaveId"].asLong(),
                behandlingId = this["behandlingId"].asUuid(),
                skalTildeles = this["skalTildeles"].asBoolean(),
                frist = this["frist"].asLocalDate(),
                opprettet = this["@opprettet"].asLocalDateTime(),
                saksbehandlerOid = this["saksbehandlerOid"].asUuid(),
                saksbehandlerIdent = this["saksbehandlerIdent"].asText(),
                notatTekst = this["notatTekst"]?.asText(),
                årsaker = this["årsaker"].map {
                    LagtPåVentÅrsak(
                        key = it["key"].asText(),
                        årsak = it["årsak"].asText()
                    )
                }
            )
        }

        fun Session.lagreLagtPåVent(lagtPåVent: LagtPåVent) {
            @Language("PostgreSQL")
            val statement = """
                INSERT INTO lagt_paa_vent(
                oppgave_id,
                behandling_id,
                skal_tildeles,
                frist,
                opprettet,
                saksbehandler_oid,
                saksbehandler_ident,
                notat_tekst
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """.trimIndent()
            val id = run(
                queryOf(
                    statement,
                    lagtPåVent.oppgaveId,
                    lagtPåVent.behandlingId,
                    lagtPåVent.skalTildeles,
                    lagtPåVent.frist,
                    lagtPåVent.opprettet,
                    lagtPåVent.saksbehandlerOid,
                    lagtPåVent.saksbehandlerIdent,
                    lagtPåVent.notatTekst
                ).asUpdateAndReturnGeneratedKey
            )
            @Language("PostgreSQL")
            val årsakStatement = """
                INSERT INTO lagt_paa_vent_arsak(
                lagt_paa_vent_id, key, arsak
                ) VALUES (?, ?, ?)
            """.trimIndent()
            lagtPåVent.årsaker.forEach { årsak ->
                run(
                    queryOf(
                        årsakStatement,
                        id,
                        årsak.key,
                        årsak.årsak
                    ).asUpdate
                )
            }
        }

    }
}

