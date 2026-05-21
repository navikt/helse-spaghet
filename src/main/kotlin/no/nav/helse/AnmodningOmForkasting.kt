package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers.isMissingOrNull
import kotliquery.Session
import kotliquery.queryOf
import no.nav.helse.Util.asNullableText
import no.nav.helse.Util.asUuid
import no.nav.helse.Util.toJson
import org.intellij.lang.annotations.Language
import java.time.LocalDateTime
import java.util.UUID

data class AnmodningOmForkasting(
    val vedtaksperiodeId: UUID,
    val fødselsnummer: String,
    val organisasjonsnummer: String,
    val yrkesaktivitetstype: String,
    val avsender: String,
    val årsaker: List<String>,
    val kommentar: String?,
    val opprettet: LocalDateTime,
) {
    companion object {
        fun JsonNode.parseAnmodningOmForkasting(): AnmodningOmForkasting? {
            val saksbehandlerIdent = this["saksbehandlerIdent"]?.takeUnless { it.isMissingOrNull() }?.asText()
            val avsenderNavIdent = this["@avsender"]?.get("NAVIdent")?.asText()
            val avsender = saksbehandlerIdent ?: avsenderNavIdent ?: return null
            return AnmodningOmForkasting(
                vedtaksperiodeId = this["vedtaksperiodeId"].asUuid(),
                fødselsnummer = this["fødselsnummer"].asText(),
                organisasjonsnummer = this["organisasjonsnummer"].asText(),
                yrkesaktivitetstype = this["yrkesaktivitetstype"].asText(),
                avsender = avsender,
                årsaker = this["årsaker"].map { it.asText() },
                kommentar = this["kommentar"].asNullableText(),
                opprettet = this["@opprettet"].asLocalDateTime(),
            )
        }

        fun Session.insertAnmodningOmForkasting(anmodning: AnmodningOmForkasting) {
            @Language("PostgreSQL")
            val statement = """
                INSERT INTO anmodning_om_forkasting(
                    vedtaksperiode_id,
                    fødselsnummer,
                    organisasjonsnummer,
                    yrkesaktivitetstype,
                    avsender,
                    årsaker,
                    kommentar,
                    opprettet
                ) VALUES (:vedtaksperiodeId, :fodselsnummer, :organisasjonsnummer, :yrkesaktivitetstype, :avsender, :arsaker::jsonb, :kommentar, :opprettet)
            """
            run(
                queryOf(
                    statement,
                    mapOf(
                        "vedtaksperiodeId" to anmodning.vedtaksperiodeId,
                        "fodselsnummer" to anmodning.fødselsnummer,
                        "organisasjonsnummer" to anmodning.organisasjonsnummer,
                        "yrkesaktivitetstype" to anmodning.yrkesaktivitetstype,
                        "avsender" to anmodning.avsender,
                        "arsaker" to anmodning.årsaker.toJson(),
                        "kommentar" to anmodning.kommentar,
                        "opprettet" to anmodning.opprettet,
                    ),
                ).asUpdate,
            )
        }
    }
}
