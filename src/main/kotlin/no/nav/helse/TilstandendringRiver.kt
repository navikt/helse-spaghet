package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import no.nav.helse.rapids_rivers.asLocalDateTime
import org.intellij.lang.annotations.Language
import java.lang.Exception
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
                it.requireKey("vedtaksperiodeId", "aktivitetslogg")
            }

        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        val json = objectMapper.readTree(packet.toJson())
        val vedtaksperiodeId = UUID.fromString(json["vedtaksperiodeId"].asText())
        try {
            //log.info("Inserter tilstandsendring for vedtaksperiodeId: $vedtaksperiodeId")
            // Vi går ut i fra at første entry i kontekster er typen hendelse som førte til endringen.
            val kildeType = json
                    .valueOrNull("aktivitetslogg")
                    ?.valueOrNull("kontekster")
                    ?.firstOrNull()?.get("kontekstType")
                    ?.asText() ?: "Ukjent"
            insertTilstandsendring(
                    hendelseId = UUID.fromString(json["@id"].asText()),
                    vedtaksperiodeId = vedtaksperiodeId,
                    tidsstempel = json["@opprettet"].asLocalDateTime(),
                    tilstandFra = json["forrigeTilstand"].asText(),
                    tilstandTil = json["gjeldendeTilstand"].asText(),
                    kilde = UUID.fromString(json["@forårsaket_av"]["id"].asText()),
                    kildeType = kildeType
            )
        } catch (e: Exception) {
            log.error("Feilet ved inserting av tilstandsendring med vedtaksperiodeId=$vedtaksperiodeId", e)
        }
    }

    private fun insertTilstandsendring(
            hendelseId: UUID,
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
    hendelse_id,
    vedtaksperiode_id,
    tidsstempel,
    tilstand_fra,
    tilstand_til,
    kilde,
    kilde_type)
VALUES(
    :hendelse_id,
    :vedtaksperiode_id,
    :tidsstempel,
    :tilstand_fra,
    :tilstand_til,
    :kilde,
    :kilde_type
);"""
            session.run(queryOf(query, mapOf(
                    "hendelse_id" to hendelseId,
                    "vedtaksperiode_id" to vedtaksperiodeId,
                    "tidsstempel" to tidsstempel,
                    "tilstand_fra" to tilstandFra,
                    "tilstand_til" to tilstandTil,
                    "kilde" to kilde,
                    "kilde_type" to kildeType
            )).asUpdate)
        }
    }
}