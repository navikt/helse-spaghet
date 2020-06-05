package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import io.prometheus.client.Counter
import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import no.nav.helse.rapids_rivers.asLocalDateTime
import java.time.LocalDateTime
import java.util.UUID
import javax.sql.DataSource

class GodkjenningLøsning(
    val vedtaksperiodeId: UUID,
    val aktørId: String,
    val fødselsnummer: String,
    val warnings: List<String>,
    val godkjenning: Godkjenning
) {
    class Factory(rapid: RapidsConnection, private val dataSource: DataSource) : River.PacketListener {
        init {
            River(rapid).apply {
                validate {
                    it.demandAll("@behov", listOf("Godkjenning"))
                    it.demandValue("@final", true)
                    it.require("@løsning.Godkjenning", ::tilGodkjenning)
                    it.requireKey("warnings", "vedtaksperiodeId", "aktørId", "fødselsnummer")
                }
            }.register(this)
        }

        val godkjentCounter = Counter.build(
            "vedtaksperioder_godkjent",
            "Antall godkjente vedtaksperioder"
        ).register()

        val årsakCounter = Counter.build(
            "vedtaksperioder_avvist_arsak",
            "Antall avviste vedtaksperioder"
        )
            .labelNames("arsak")
            .register()

        val begrunnelserCounter = Counter.build(
            "vedtaksperioder_avvist_begrunnelser",
            "Antall avviste vedtaksperioder"
        )
            .labelNames("begrunnelse")
            .register()

        override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
            val løsning = GodkjenningLøsning(
                vedtaksperiodeId = UUID.fromString(packet["vedtaksperiodeId"].asText()),
                fødselsnummer = packet["fødselsnummer"].asText(),
                aktørId = packet["aktørId"].asText(),
                warnings = packet["warnings"].warnings(),
                godkjenning = tilGodkjenning(packet["@løsning.Godkjenning"])
            )

            log.info("Lagrer godkjenning for {}", keyValue("vedtaksperiodeId", løsning.vedtaksperiodeId))

            if (løsning.godkjenning.godkjent) {
                godkjentCounter.inc()
            } else {
                if (løsning.godkjenning.årsak != null) {
                    årsakCounter.labels(løsning.godkjenning.årsak).inc()
                }
                løsning.godkjenning.begrunnelser?.forEach { begrunnelserCounter.labels(it).inc() }
            }

            dataSource.insertGodkjenning(løsning)
        }

        private fun tilGodkjenning(jsonNode: JsonNode) = Godkjenning(
            godkjent = jsonNode["godkjent"].asBoolean(),
            saksbehandlerIdent = jsonNode["saksbehandlerIdent"].asText(),
            godkjentTidspunkt = jsonNode["godkjenttidspunkt"].asLocalDateTime(),
            årsak = jsonNode.optional("årsak")?.asText(),
            begrunnelser = jsonNode.optional("begrunnelser")?.map(JsonNode::asText),
            kommentar = jsonNode.optional("kommentar")?.asText()?.takeIf { it.isNotBlank() }
        )

        private fun JsonNode.optional(name: String) = takeIf { hasNonNull(name) }?.get(name)

        private fun JsonNode.warnings() = this["aktiviteter"].map { it["melding"].asText() }
    }

    data class Godkjenning(
        val godkjent: Boolean,
        val saksbehandlerIdent: String,
        val godkjentTidspunkt: LocalDateTime,
        val årsak: String?,
        val begrunnelser: List<String>?,
        val kommentar: String?
    )
}
