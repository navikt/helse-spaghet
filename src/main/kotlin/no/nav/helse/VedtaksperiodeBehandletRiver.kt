package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import kotliquery.Session
import kotliquery.queryOf
import kotliquery.sessionOf
import net.logstash.logback.argument.StructuredArguments.keyValue
import org.intellij.lang.annotations.Language
import org.postgresql.util.PSQLException
import java.time.LocalDateTime
import java.util.*
import javax.sql.DataSource

class VedtaksperiodeBehandletRiver(
    rapidApplication: RapidsConnection,
    private val dataSource: DataSource
) : River.PacketListener {
    init {
        River(rapidApplication).apply {
            precondition {
                it.requireAll("@behov", listOf("Godkjenning"))
                it.forbid("@final")
            }
            validate {
                it.requireKey("@behovId", "vedtaksperiodeId", "@løsning")
            }

        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        val json = objectMapper.readTree(packet.toJson())
        val behovId = UUID.fromString(json["@behovId"].asText())
        val vedtaksperiodeId = UUID.fromString(json["vedtaksperiodeId"].asText())
        val løsning = løsning(json)
        val saksbehandlerIdentitet = finnIdentitet(løsning)
        try {
            sessionOf(dataSource).use { session ->
                insertLøsning(session, behovId, hentGodkjentTidspunkt(json), saksbehandlerIdentitet, løsning)
                if (løsning.hasNonNull("begrunnelser")) {
                    insertBegrunnelser(session, behovId, løsning)
                }
            }
            logg.info("Lagret løsning for godkjenningsbehov for vedtaksperiodeId=$vedtaksperiodeId")
        } catch (err: PSQLException) {
            logg.warn("Klarte ikke lagre løsning for godkjenningsbehov {}, mest sannsynlig fordi opprinnelig behov ikke er lagret (eller at vi inserter løsning med feil foreign key): {}", keyValue("behovId", behovId), err.message, err)
        }
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
