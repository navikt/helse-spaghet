package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import io.prometheus.client.Histogram
import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import no.nav.helse.rapids_rivers.*
import org.intellij.lang.annotations.Language
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.time.temporal.ChronoUnit
import java.time.temporal.ChronoUnit.MILLIS
import java.util.*
import javax.sql.DataSource

class TidFraGodkjenningTilUtbetalingRiver(
    rapidApplication: RapidsConnection,
    private val dataSource: DataSource
) : River.PacketListener {

    init {
        River(rapidApplication).apply {
            validate {
                it.demandValue("@event_name", "vedtaksperiode_endret")
                it.requireKey("vedtaksperiodeId")
                it.demandValue("gjeldendeTilstand", "AVSLUTTET")
                it.demandValue("forrigeTilstand", "TIL_UTBETALING")
            }

        }.register(this)
    }

    companion object {
        val utbetalingsRTT = Histogram
            .build("tidBrukt", "Måler hvor lang tid det tar fra godkjenning til utbetaling")
            .register()
        val legacyDateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val json = objectMapper.readTree(packet.toJson())
        val vedtaksperiodeId = UUID.fromString(json["vedtaksperiodeId"].asText())

        finnUtbetalingsTidspunkt(json)?.let { utbetalingsTidspunkt ->
            finnGodkjenninger(vedtaksperiodeId)?.let { godkjentTidspunkt ->
                val delta = MILLIS.between(utbetalingsTidspunkt, godkjentTidspunkt)
                utbetalingsRTT.observe(delta.toDouble())
            }
        }
    }

    private fun finnUtbetalingsTidspunkt(json: JsonNode) = json
        .valueOrNull("aktivitetslogg")
        ?.valueOrNull("aktiviteter")
        ?.find { node -> "OK fra Oppdragssystemet".equals(node.valueOrNull("melding")?.textValue()) }
        ?.valueOrNull("tidsstempel")?.fromDate()

    private fun finnGodkjenninger(vedtaksperiodeId: UUID) = using(sessionOf(dataSource)) { session ->
        session.run(
            queryOf("SELECT * FROM godkjenning WHERE vedtaksperiode_id=?;", vedtaksperiodeId)
                .map {
                    it.localDateTime("godkjent_tidspunkt")
                }.asSingle
        )
    }

    private fun JsonNode.fromDate(): LocalDateTime =
        try {
            this.asLocalDateTime()
        } catch (_: DateTimeParseException) {
            LocalDateTime.from(TidFraGodkjenningTilUtbetalingRiver.legacyDateFormat.parse(this.asText()))
        }

}