package no.nav.helse

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
                it.requireKey("vedtaksperiodeId", "aktivitetslogg", "@id")
                it.requireKey("forrigeTilstand", "gjeldendeTilstand")
                it.demand("forrigeTilstand") { forrigeTilstand ->
                    require(forrigeTilstand.textValue() != it["gjeldendeTilstand"].textValue())
                }
            }

        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val json = objectMapper.readTree(packet.toJson())
        val vedtaksperiodeId = UUID.fromString(json["vedtaksperiodeId"].asText())
        val kildeType = json
            .valueOrNull("aktivitetslogg")
            ?.valueOrNull("kontekster")
            ?.firstOrNull()?.get("kontekstType")
            ?.asText() ?: "Ukjent"
        insertTilstandsendring(
            id = UUID.fromString(json["@id"].asText()),
            vedtaksperiodeId = vedtaksperiodeId,
            tidsstempel = json["@opprettet"].asLocalDateTime(),
            tilstandFra = json["forrigeTilstand"].asText(),
            tilstandTil = json["gjeldendeTilstand"].asText(),
            kilde = UUID.fromString(json["@forårsaket_av"]["id"].asText()),
            kildeType = kildeType
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