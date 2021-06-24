package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.*
import org.intellij.lang.annotations.Language
import java.lang.Exception
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.util.*
import javax.sql.DataSource

class AktivitetRiver(
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

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val json = objectMapper.readTree(packet.toJson())
        val vedtaksperiodeId = UUID.fromString(json["vedtaksperiodeId"].asText())
        try {
            //log.info("Inserter aktiviteter for vedtaksperiodeId: ${json["vedtaksperiodeId"].asText()}")
            json["aktivitetslogg"]["aktiviteter"].forEach { aktivitet ->
                insertAktivitet(
                        id = UUID.fromString(json["@id"].asText()),
                        vedtaksperiodeId = vedtaksperiodeId,
                        melding = aktivitet["melding"].asText(),
                        level = aktivitet["alvorlighetsgrad"].asText(),
                        tidsstempel = aktivitet["tidsstempel"].fromDate(),
                        kilde = UUID.fromString(json["@forårsaket_av"]["id"].asText())
                )
            }
        } catch (e: Exception) {
            log.error("Feilet ved inserting av aktiviteter for vedtaksperiode=$vedtaksperiodeId", e)
        }
    }


    private fun JsonNode.fromDate(): LocalDateTime =
            try {
                this.asLocalDateTime()
            } catch (_: DateTimeParseException) {
                LocalDateTime.from(legacyDateFormat.parse(this.asText()))
            }


    private fun insertAktivitet(
        id: UUID,
        vedtaksperiodeId: UUID,
        melding: String,
        level: String,
        tidsstempel:
            LocalDateTime,
        kilde: UUID
    ) {
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = """INSERT INTO vedtaksperiode_aktivitet(id, vedtaksperiode_id, melding, level, tidsstempel, kilde) VALUES(:id, :vedtaksperiode_id, :melding, :level, :tidsstempel, :kilde) ON CONFLICT DO NOTHING"""
            session.run(queryOf(query, mapOf(
                    "id" to id,
                    "vedtaksperiode_id" to vedtaksperiodeId,
                    "melding" to melding,
                    "level" to level,
                    "tidsstempel" to tidsstempel,
                    "kilde" to kilde
            )).asUpdate)
        }
    }

    companion object {
        // Spleis serialiserer ikke datoer i aktivitetsloggeren som ISO-8601, men bruker sitt eget format 😞
        val legacyDateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
    }
}