package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import no.nav.helse.rapids_rivers.asLocalDateTime
import org.intellij.lang.annotations.Language
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*
import javax.sql.DataSource

class VedtaksperiodeTilGodkjenningRiver(
    rapidApplication: RapidsConnection,
    private val dataSource: DataSource
) : River.PacketListener {
    init {
        River(rapidApplication).apply {
            validate {
                //it.demandValue("@event_name", "behov")
                it.requireKey("@id", "@opprettet")
                it.interestedIn("vedtaksperiodeId")
                it.demandAll("@behov", listOf("Godkjenning"))
                it.forbid("@løsning")
                it.forbid("@final")
            }

        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        try {
            val json = objectMapper.readTree(packet.toJson())
            val vedtaksperiodeId = UUID.fromString(json["vedtaksperiodeId"].asText())
            val behovOpprettet = json["@opprettet"].asLocalDateTime()
            val id = UUID.fromString(json["@id"].asText())
            sessionOf(dataSource).use { session ->
                insertGodkjenningsbehov(id, periodetype(json), vedtaksperiodeId, behovOpprettet)
            }
            log.info("Lagret godkjenningsbehov for vedtaksperiodeId=$vedtaksperiodeId")
        } catch (e: Exception) {
            log.error("Feilet ved inserting av godkjenningsbehov", e)
        }
    }

    fun periodetype(json: JsonNode) = when {
        // Gamle godkjenningsbehov
        json.hasNonNull("periodetype") -> json["periodetype"].asText()
        // Nytt format flytter periodetype i Godkjenning
        json.hasNonNull("Godkjenning") ->  json["Godkjenning"]["periodetype"].asText()
        // Historiske godkjenningsbehov har ikke periodetype (før automatisering)
        else -> "UKJENT"
    }


    private fun insertGodkjenningsbehov(hendelseId: UUID, periodetype: String?, vedtaksperiodeId: UUID, tidspunkt: LocalDateTime) {
        @Language("PostgreSQL")
        val insertGodkjenningsbehov =
            "INSERT INTO godkjenningsbehov(id, vedtaksperiode_id, periodetype, tidspunkt) VALUES(:id, :vedtaksperiode_id, :periodetype, :tidspunkt) ON CONFLICT DO NOTHING;"
        sessionOf(dataSource).use { session ->
            session.run(
                queryOf(
                    insertGodkjenningsbehov, mapOf(
                        "id" to hendelseId,
                        "vedtaksperiode_id" to vedtaksperiodeId,
                        "periodetype" to periodetype,
                        "tidspunkt" to tidspunkt
                    )
                ).asUpdate
            )
        }
    }
}
