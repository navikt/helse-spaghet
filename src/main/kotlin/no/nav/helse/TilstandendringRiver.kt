package no.nav.helse

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import kotliquery.queryOf
import kotliquery.sessionOf
import org.intellij.lang.annotations.Language
import tools.jackson.databind.JsonNode
import java.time.LocalDateTime
import java.util.*
import javax.sql.DataSource

class TilstandendringRiver(
    rapidApplication: RapidsConnection,
    private val dataSource: DataSource,
) : River.PacketListener {
    init {
        River(rapidApplication)
            .apply {
                precondition {
                    it.requireValue("@event_name", "vedtaksperiode_endret")
                    it.requireKey("gjeldendeTilstand")
                    it.require("forrigeTilstand") { forrigeTilstand ->
                        require(forrigeTilstand.stringValue(null) != it["gjeldendeTilstand"].stringValue(null))
                    }
                }
                validate {
                    it.requireKey("vedtaksperiodeId", "@id", "behandlingId")
                    it.require("@opprettet", JsonNode::asLocalDateTime)
                    it.requireKey("@forårsaket_av.id", "@forårsaket_av.event_name")
                    it.requireKey("forrigeTilstand", "gjeldendeTilstand")
                }
            }.register(this)
    }

    override fun onPacket(
        packet: JsonMessage,
        context: MessageContext,
        metadata: MessageMetadata,
        meterRegistry: MeterRegistry,
    ) {
        val vedtaksperiodeId = UUID.fromString(packet["vedtaksperiodeId"].asString())
        val behandlingId = UUID.fromString(packet["behandlingId"].asString())
        insertTilstandsendring(
            id = UUID.fromString(packet["@id"].asString()),
            vedtaksperiodeId = vedtaksperiodeId,
            behandlingId = behandlingId,
            tidsstempel = packet["@opprettet"].asLocalDateTime(),
            tilstandFra = packet["forrigeTilstand"].asString(),
            tilstandTil = packet["gjeldendeTilstand"].asString(),
            kilde = UUID.fromString(packet["@forårsaket_av.id"].asString()),
            kildeType = packet["@forårsaket_av.event_name"].asString(),
        )
    }

    private fun insertTilstandsendring(
        id: UUID,
        vedtaksperiodeId: UUID,
        behandlingId: UUID,
        tidsstempel: LocalDateTime,
        tilstandFra: String,
        tilstandTil: String,
        kilde: UUID,
        kildeType: String,
    ) {
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = """
INSERT INTO vedtaksperiode_tilstandsendring(
    id,
    vedtaksperiode_id,
    behandling_id,
    tidsstempel,
    tilstand_fra,
    tilstand_til,
    kilde,
    kilde_type)
VALUES(
    :id,
    :vedtaksperiode_id,
    :behandling_id,
    :tidsstempel,
    :tilstand_fra,
    :tilstand_til,
    :kilde,
    :kilde_type
) ON CONFLICT DO NOTHING;"""
            session.run(
                queryOf(
                    query,
                    mapOf(
                        "id" to id,
                        "vedtaksperiode_id" to vedtaksperiodeId,
                        "behandling_id" to behandlingId,
                        "tidsstempel" to tidsstempel,
                        "tilstand_fra" to tilstandFra,
                        "tilstand_til" to tilstandTil,
                        "kilde" to kilde,
                        "kilde_type" to kildeType,
                    ),
                ).asUpdate,
            )
        }
    }
}
