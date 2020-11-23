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
        try {
            val json = objectMapper.readTree(packet.toJson())
            log.info("Inserter tilstandsendring for vedtaksperiodeId: ${json["vedtaksperiodeId"].asText()}")
            // Vi går ut i fra at første entry i kontekster er typen hendelse som førte til endringen.
            val kildeType = json["aktivitetslogg"]["kontekster"].first()["kontekstType"].asText()
            insertTilstandsendring(
                    vedtaksperiodeId = UUID.fromString(json["vedtaksperiodeId"].asText()),
                    tidsstempel = json["@opprettet"].asLocalDateTime(),
                    tilstandFra = json["forrigeTilstand"].asText(),
                    tilstandTil = json["gjeldendeTilstand"].asText(),
                    kilde = UUID.fromString(json["@forårsaket_av"]["id"].asText()),
                    kildeType = kildeType
            )
        } catch (e: Exception) {
            log.error("Feilet ved inserting av tilstandsendring", e)
        }
    }

    private fun insertTilstandsendring(
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
    vedtaksperiode_id,
    tidsstempel,
    tilstand_fra,
    tilstand_til,
    kilde,
    kilde_type)
VALUES(
    :vedtaksperiode_id,
    :tidsstempel,
    :tilstand_fra,
    :tilstand_til,
    :kilde,
    :kilde_type
);"""
            session.run(queryOf(query, mapOf(
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