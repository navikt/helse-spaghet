package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.*
import org.intellij.lang.annotations.Language
import java.time.LocalDateTime
import java.util.*
import javax.sql.DataSource

class TilstandendringRiver(
    rapidApplication: RapidsConnection,
    private val dataSource: DataSource
) : River.PacketListener {

    init {
        River(rapidApplication).apply {
            validate {
                it.demandValue("@event_name", "vedtaksperiode_endret")
                it.requireKey("vedtaksperiodeId", "@id")
                it.require("@opprettet", JsonNode::asLocalDateTime)
                it.requireKey("@forårsaket_av.id", "@forårsaket_av.event_name")
                it.requireKey("forrigeTilstand", "gjeldendeTilstand")
                it.demand("forrigeTilstand") { forrigeTilstand ->
                    require(forrigeTilstand.textValue() != it["gjeldendeTilstand"].textValue())
                }
            }

        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val vedtaksperiodeId = UUID.fromString(packet["vedtaksperiodeId"].asText())
        insertTilstandsendring(
            id = UUID.fromString(packet["@id"].asText()),
            vedtaksperiodeId = vedtaksperiodeId,
            tidsstempel = packet["@opprettet"].asLocalDateTime(),
            tilstandFra = packet["forrigeTilstand"].asText(),
            tilstandTil = packet["gjeldendeTilstand"].asText(),
            kilde = UUID.fromString(packet["@forårsaket_av.id"].asText()),
            kildeType = packet["@forårsaket_av.event_name"].asText()
        )
    }

    private fun insertTilstandsendring(
        id: UUID,
        vedtaksperiodeId: UUID,
        tidsstempel: LocalDateTime,
        tilstandFra: String,
        tilstandTil: String,
        kilde: UUID,
        kildeType: String
    ) {
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = """
INSERT INTO vedtaksperiode_tilstandsendring(
    id,
    vedtaksperiode_id,
    tidsstempel,
    tilstand_fra,
    tilstand_til,
    kilde,
    kilde_type)
VALUES(
    :id,
    :vedtaksperiode_id,
    :tidsstempel,
    :tilstand_fra,
    :tilstand_til,
    :kilde,
    :kilde_type
) ON CONFLICT DO NOTHING;"""
            session.run(
                queryOf(
                    query, mapOf(
                        "id" to id,
                        "vedtaksperiode_id" to vedtaksperiodeId,
                        "tidsstempel" to tidsstempel,
                        "tilstand_fra" to tilstandFra,
                        "tilstand_til" to tilstandTil,
                        "kilde" to kilde,
                        "kilde_type" to kildeType
                    )
                ).asUpdate
            )
        }
    }
}