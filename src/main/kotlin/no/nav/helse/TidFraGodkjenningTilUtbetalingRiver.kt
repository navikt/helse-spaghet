package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import io.prometheus.client.Summary
import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import no.nav.helse.rapids_rivers.*
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
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
        val utbetalingsRTT = Summary.build()
            .name("tidBrukt")
            .help("Latency inntektskomponenten, in seconds")
            .register()
        val utbetalingRttCustomQuantiles = Summary.build()
            .name("tidBruktCustomQuantiles")
            .quantile(0.5, 0.05) // Add 50th percentile (= median) with 5% tolerated error
            .quantile(0.9, 0.01) // Add 90th percentile with 1% tolerated error
            .quantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
            .help("Latency inntektskomponenten, in seconds")
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
                utbetalingRttCustomQuantiles.observe(delta.toDouble())
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
