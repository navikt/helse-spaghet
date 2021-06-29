package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import kotliquery.Session
import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.Util.asNullableText
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import org.intellij.lang.annotations.Language
import java.util.*
import javax.sql.DataSource

class VedtaksperiodeGodkjentRiver(
    rapidApplication: RapidsConnection,
    private val dataSource: DataSource
) : River.PacketListener {
    init {
        River(rapidApplication).apply {
            validate {
                it.demand("@event_name") { node ->
                    listOf(
                        "vedtaksperiode_godkjent",
                        "vedtaksperiode_avvist"
                    ).contains(node.asNullableText())
                }
                it.requireKey("@id", "vedtaksperiodeId", "warnings")
            }
        }.register(this)
    }


    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val json = objectMapper.readTree(packet.toJson())
        val vedtaksperiodeId = UUID.fromString(json["vedtaksperiodeId"].asText())
        val warnings = json["warnings"].map(::warningOfJson)
        if (warnings.isEmpty()) {
            return
        }
        sessionOf(dataSource).use { session ->
            val godkjenningsId = session.findGodkjenning(vedtaksperiodeId)
            warnings.forEach {
                session.insertWarning(godkjenningsId, it)
            }
            log.info("Lagret warnings for godkjenningsId=$godkjenningsId, vedtaksPeriode=$vedtaksperiodeId")
        }
    }


    data class Warning(
        val melding: String,
        val kilde: String
    )

    companion object {
        private fun warningOfJson(json: JsonNode) =
            Warning(melding = json["melding"].asText(), kilde = json["kilde"].asText())

        private fun Session.insertWarning(
            godkjenningsId: Int,
            warning: Warning
        ): Int {
            @Language("PostgreSQL")
            val statement = """
INSERT INTO warning_for_godkjenning(godkjenning_ref, melding, kilde)
VALUES (:godkjenningsId, :melding, :kilde)
ON CONFLICT (godkjenning_ref, melding) DO NOTHING;
"""
            return run(
                queryOf(
                    statement, mapOf(
                        "godkjenningsId" to godkjenningsId,
                        "melding" to warning.melding,
                        "kilde" to warning.kilde
                    )
                ).asUpdate
            )
        }
    }
}