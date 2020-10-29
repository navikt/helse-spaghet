package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import kotliquery.Session
import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.*
import org.intellij.lang.annotations.Language
import java.util.*
import javax.sql.DataSource

class VedtaksperiodeBehandletRiver(
    rapidApplication: RapidsConnection,
    private val dataSource: DataSource
) : River.PacketListener {
    init {
        River(rapidApplication).apply {
            validate {
                it.demandValue("@event_name", "behov")
                it.demandAll("@behov", listOf("Godkjenning"))
                it.requireValue("@final", true)
                it.requireKey("@id", "vedtaksperiodeId", "@løsning")
            }

        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        try {
            val json = objectMapper.readTree(packet.toJson())
            val id = UUID.fromString(json["@id"].asText())
            val vedtaksperiodeId = UUID.fromString(json["vedtaksperiodeId"].asText())
            val løsning = json["@løsning"]["Godkjenning"]
            sessionOf(dataSource).use { session ->
                insertLøsning(session, id, løsning)
                insertBegrunnelser(session, id, løsning)
                insertWarnings(session, json, vedtaksperiodeId)
            }
            log.info("Lagret løsning for godkjenningsbehov for vedtaksperiodeId=$vedtaksperiodeId")
        } catch (e: Exception) {
            log.error("Feilet ved inserting av løsning for godkjenningsbehov", e)
        }
    }

    private fun insertLøsning(
        session: Session,
        id: UUID,
        løsning: JsonNode
    ) {
        @Language("PostgreSQL")
        val løsningInsert =
            """INSERT INTO godkjenningsbehov_losning(id, godkjent, automatisk_behandling, arsak, godkjenttidspunkt) VALUES(:id, :godkjent, :automatisk_behandling, :arsak, :godkjenttidspunkt);"""
        session.run(
            queryOf(
                løsningInsert, mapOf(
                    "id" to id,
                    "godkjent" to løsning["godkjent"].asBoolean(),
                    "automatisk_behandling" to (løsning["automatiskBehandling"]?.asBoolean(false) ?: false),
                    "arsak" to løsning["årsak"]?.takeIf { !it.isMissingOrNull() }?.asText(),
                    "godkjenttidspunkt" to løsning["godkjenttidspunkt"].asLocalDateTime()
                )
            ).asUpdate
        )
    }

    private fun insertWarnings(
        session: Session,
        json: JsonNode,
        vedtaksperiodeId: UUID
    ) {
        @Language("PostgreSQL")
        val warningInsert = "INSERT INTO godkjenningsbehov_warning(vedtaksperiode_id, melding) VALUES(:vedtaksperiode_id, :warning) ON CONFLICT DO NOTHING;"
        json["warnings"]["aktiviteter"].forEach { warning ->
            session.run(
                queryOf(
                    warningInsert, mapOf(
                        "vedtaksperiode_id" to vedtaksperiodeId,
                        "warning" to warning["melding"].asText()
                    )
                ).asUpdate
            )
        }
    }

    private fun insertBegrunnelser(
        session: Session,
        id: UUID?,
        løsning: JsonNode
    ) {
        val begrunnelser = løsning["begrunnelser"] ?: return
        @Language("PostgreSQL")
        val begrunnelseInsert = "INSERT INTO godkjenningsbehov_losning_begrunnelse(id, begrunnelse) VALUES(:id, :begrunnelse);"
        begrunnelser.forEach { begrunnelse ->
            session.run(
                queryOf(
                    begrunnelseInsert, mapOf(
                        "id" to id,
                        "begrunnelse" to begrunnelse.asText()
                    )
                ).asUpdate
            )
        }
    }
}