package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import kotliquery.Session
import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.*
import org.intellij.lang.annotations.Language
import java.time.LocalDateTime
import java.util.*
import javax.sql.DataSource

class VedtaksperiodeBehandletRiver(
    rapidApplication: RapidsConnection,
    private val dataSource: DataSource
) : River.PacketListener {
    init {
        River(rapidApplication).apply {
            validate {
                it.demandAll("@behov", listOf("Godkjenning"))
                it.rejectKey("@final")
                it.requireKey("@id", "vedtaksperiodeId", "@løsning")
            }

        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val json = objectMapper.readTree(packet.toJson())
        val id = UUID.fromString(json["@id"].asText())
        val vedtaksperiodeId = UUID.fromString(json["vedtaksperiodeId"].asText())
        val løsning = løsning(json)
        val saksbehandlerIdentitet = finnIdentitet(løsning)
        sessionOf(dataSource).use { session ->
            insertLøsning(session, id, hentGodkjentTidspunkt(json), saksbehandlerIdentitet, løsning)
            if (løsning.hasNonNull("begrunnelser")) {
                insertBegrunnelser(session, id, løsning)
            }
        }
        log.info("Lagret løsning for godkjenningsbehov for vedtaksperiodeId=$vedtaksperiodeId")
    }

    private fun hentGodkjentTidspunkt(json: JsonNode): LocalDateTime {
        // godkjenningstidspunkt har blitt flyttet til løsningen
        return (løsning(json)["godkjenttidspunkt"]?.asLocalDateTime() ?: json["godkjenttidspunkt"].asLocalDateTime())
    }

    private fun finnIdentitet(løsning: JsonNode) = when {
        løsning.valueOrNull("automatiskBehandling")?.asBoolean() == true -> SPESIALIST_OID
        else -> løsning["saksbehandlerIdent"].asText()
    }

    private fun løsning(json: JsonNode): JsonNode {
        return json["@løsning"]["Godkjenning"]
    }

    private fun insertLøsning(
        session: Session,
        id: UUID,
        godkjentTidspunkt: LocalDateTime,
        saksbehandlerIdentitet: String,
        løsning: JsonNode
    ) {
        @Language("PostgreSQL")
        val løsningInsert =
            """INSERT INTO godkjenningsbehov_losning(id, godkjent, automatisk_behandling, arsak, godkjent_av, godkjenttidspunkt) VALUES(:id, :godkjent, :automatisk_behandling, :arsak, :godkjent_av, :godkjenttidspunkt) ON CONFLICT DO NOTHING;"""
        session.run(
            queryOf(
                løsningInsert, mapOf(
                    "id" to id,
                    "godkjent" to løsning["godkjent"].asBoolean(),
                    "automatisk_behandling" to (løsning["automatiskBehandling"]?.asBoolean(false) ?: false),
                    "arsak" to løsning.valueOrNull("årsak")?.asText(),
                    "godkjent_av" to saksbehandlerIdentitet,
                    "godkjenttidspunkt" to godkjentTidspunkt
                )
            ).asUpdate
        )
    }

    private fun insertBegrunnelser(
        session: Session,
        id: UUID?,
        løsning: JsonNode
    ) {
        val begrunnelser = løsning["begrunnelser"]
        @Language("PostgreSQL")
        val begrunnelseInsert = "INSERT INTO godkjenningsbehov_losning_begrunnelse(id, begrunnelse) VALUES(:id, :begrunnelse) ON CONFLICT DO NOTHING;"
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

    companion object {
        const val SPESIALIST_OID = "5cc46653-7c6c-4c5e-b617-baf648452ad4"
    }
}
