package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import kotliquery.queryOf
import kotliquery.sessionOf
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
            precondition {
                it.requireValue("@event_name", "vedtaksperiode_endret")
                it.requireValue("gjeldendeTilstand", "AVSLUTTET")
                it.requireValue("forrigeTilstand", "TIL_UTBETALING")
            }
            validate {
                it.requireKey("vedtaksperiodeId")
            }

        }.register(this)
    }

    companion object {
        val legacyDateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

    }

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        val json = objectMapper.readTree(packet.toJson())
        val vedtaksperiodeId = UUID.fromString(json["vedtaksperiodeId"].asText())

        finnUtbetalingsTidspunkt(json)?.let { utbetalingsTidspunkt ->
            finnGodkjenninger(vedtaksperiodeId)?.also { godkjentTidspunkt ->
                val delta = MILLIS.between(godkjentTidspunkt, utbetalingsTidspunkt)
                Timer.builder("tidBrukt")
                    .description("Måler hvor lang tid det tar fra godkjenning til utbetaling i millisekunder")
                    .publishPercentiles(0.5, 0.9, 0.99)
                    .register(meterRegistry)
                    .record(delta::toLong)
            }
        }
    }

    private fun finnUtbetalingsTidspunkt(json: JsonNode) = json
        .valueOrNull("aktivitetslogg")
        ?.valueOrNull("aktiviteter")
        ?.find { node -> "OK fra Oppdragssystemet".equals(node.valueOrNull("melding")?.textValue()) }
        ?.valueOrNull("tidsstempel")?.fromDate()

    private fun finnGodkjenninger(vedtaksperiodeId: UUID) = sessionOf(dataSource).use { session ->
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
            LocalDateTime.from(legacyDateFormat.parse(this.asText()))
        }

}
