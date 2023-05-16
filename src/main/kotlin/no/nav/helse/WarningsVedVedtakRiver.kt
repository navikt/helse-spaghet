package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import java.lang.RuntimeException
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

/**
 * Leser informasjon fra events som Spesialist sender ut samtidig som den svarer på godkjenningsbehov,
 * for å ta vare på warnings både fra Spleis og Spesialist.
 */
class WarningsVedVedtakRiver(
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
            val godkjenningsId = session.findGodkjenningId(vedtaksperiodeId)
                ?: throw RuntimeException("Forventet godkjenning for vedtaksperiode $vedtaksperiodeId")
            warnings.forEach {
                session.insertWarning(godkjenningsId, it)
            }
            logg.info("Lagret warnings for godkjenningsId=$godkjenningsId, vedtaksPeriode=$vedtaksperiodeId")
        }
    }


    data class Warning(
        val melding: String,
        val kilde: String
    )

    companion object {
        private fun warningOfJson(json: JsonNode) =
            Warning(melding = json["melding"].asText(), kilde = json["kilde"].asText())

        /**
         * Her er tanken å lagre alle warnings som ble vist til saksbehandler
         */
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
