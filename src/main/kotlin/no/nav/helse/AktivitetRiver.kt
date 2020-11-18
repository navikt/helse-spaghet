package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import no.nav.helse.rapids_rivers.asLocalDateTime
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

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        try {
            val json = objectMapper.readTree(packet.toJson())
            log.info("Inserter aktiviteter for vedtaksperiodeId: ${json["vedtaksperiodeId"].asText()}")
            json["aktivitetslogg"]["aktiviteter"].forEach { aktivitet ->
                insertAktivitet(
                        vedtaksperiodeId = UUID.fromString(json["vedtaksperiodeId"].asText()),
                        melding = aktivitet["melding"].asText(),
                        level = aktivitet["alvorlighetsgrad"].asText(),
                        tidsstempel = aktivitet["tidsstempel"].fromDate()
                )
            }
        } catch (e: Exception) {
            log.error("Feilet ved inserting av aktiviteter", e)
        }
    }

    // Spleis serialiserer ikke datoer i aktivitetsloggeren som ISO-8601, men bruker sitt eget format 😞
    private val legacyDateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
    private fun JsonNode.fromDate(): LocalDateTime =
            try {
                this.asLocalDateTime()
            } catch (_: DateTimeParseException) {
                LocalDateTime.from(legacyDateFormat.parse(this.asText()))
            }


    private fun insertAktivitet(vedtaksperiodeId: UUID, melding: String, level: String, tidsstempel: LocalDateTime) {
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = """INSERT INTO vedtaksperiode_aktivitet(vedtaksperiode_id, melding, level, tidsstempel) VALUES(:vedtaksperiode_id, :melding, :level, :tidsstempel) ON CONFLICT DO NOTHING"""
            session.run(queryOf(query, mapOf(
                    "vedtaksperiode_id" to vedtaksperiodeId,
                    "melding" to melding,
                    "level" to level,
                    "tidsstempel" to tidsstempel
            )).asUpdate)
        }
    }
}