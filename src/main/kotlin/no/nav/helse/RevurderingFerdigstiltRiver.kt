package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.*
import org.intellij.lang.annotations.Language
import java.util.*
import javax.sql.DataSource

class RevurderingFerdigstiltRiver(
    rapidApplication: RapidsConnection,
    private val dataSource: DataSource
) : River.PacketListener {
    init {
        River(rapidApplication).apply {
            validate {
                it.demandValue("@event_name", "revurdering_ferdigstilt")
                it.require("revurderingId") { id -> UUID.fromString(id.asText()) }
                it.require("@opprettet", JsonNode::asLocalDateTime)
                it.requireKey("status")
                it.requireArray("berørtePerioder") {
                    require("vedtaksperiodeId") { vedtaksperiodeId -> UUID.fromString(vedtaksperiodeId.asText()) }
                    requireKey("status")
                }
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val revurderingId = UUID.fromString(packet["revurderingId"].asText())
        val status = packet["status"].asText()
        val opprettet = packet["@opprettet"].asLocalDateTime()
        val berørtePerioder = packet["berørtePerioder"].map { periode ->
            UUID.fromString(periode.path("vedtaksperiodeId").asText()) to periode.path("status").asText()
        }

        log.info("Legger inn data fra revurdering_ferdigstilt i databasen")

        sessionOf(dataSource).use {
            it.transaction { session ->
                session.run(
                    queryOf(
                        statement = statement,
                        paramMap = mapOf(
                            "revurderingId" to revurderingId,
                            "opprettet" to opprettet,
                            "status" to status,
                        )
                    ).asUpdate
                )

                berørtePerioder.forEach { (vedtaksperiodeId, status) ->
                    session.run(queryOf(statement2, mapOf(
                        "status" to status,
                        "vedtaksperiodeId" to vedtaksperiodeId,
                        "revurderingId" to revurderingId
                    )).asExecute)
                }
            }
        }
    }

    private companion object {
        @Language("PostgreSQL")
        val statement = """ UPDATE revurdering SET status=CAST(:status as revurderingstatus), oppdatert=:oppdatert WHERE id=:revurderingId """

        @Language("PostgreSQL")
        val statement2 = """ UPDATE revurdering_vedtaksperiode SET status=CAST(:status as revurderingstatus) WHERE vedtaksperiode_id=:vedtaksperiodeId AND revurdering_id=:revurderingId """

    }
}
