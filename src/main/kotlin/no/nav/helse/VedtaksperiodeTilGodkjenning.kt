package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import org.intellij.lang.annotations.Language
import java.util.*
import javax.sql.DataSource

class VedtaksperiodeTilGodkjenningRiver(
    rapidApplication: RapidsConnection,
    private val dataSource: DataSource
) : River.PacketListener {
    init {
        River(rapidApplication).apply {
            validate {
                it.demandValue("@event_name", "behov")
                it.requireKey("@id", "vedtaksperiodeId")
                it.demandAll("@behov", listOf("Godkjenning"))
                //it.forbid("@løsning")
            }

        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        try {
            val json = objectMapper.readTree(packet.toJson())
            val vedtaksperiodeId = UUID.fromString(json["vedtaksperiodeId"].asText())
            sessionOf(dataSource).use { session ->
                insertGodkjenningsbehov(json, vedtaksperiodeId)
            }
            log.info("Lagret godkjenningsbehov for vedtaksperiodeId=$vedtaksperiodeId")
        } catch (e: Exception) {
            log.error("Feilet ved inserting av godkjenningsbehov", e)
        }
    }

    private fun insertGodkjenningsbehov(json: JsonNode, vedtaksperiodeId: UUID) {
        @Language("PostgreSQL")
        val insertGodkjenningsbehov =
            "INSERT INTO godkjenningsbehov(id, vedtaksperiode_id, periodetype) VALUES(:id, :vedtaksperiode_id, :periodetype) ON CONFLICT DO NOTHING;"
        sessionOf(dataSource).use { session ->
            session.run(
                queryOf(
                    insertGodkjenningsbehov, mapOf(
                        "id" to UUID.fromString(json["@id"].asText()),
                        "vedtaksperiode_id" to vedtaksperiodeId,
                        "periodetype" to json["periodetype"]?.asText()
                    )
                ).asUpdate
            )
        }
    }
}
