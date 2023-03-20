package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import io.prometheus.client.Counter
import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.helse.rapids_rivers.*
import java.time.LocalDateTime
import java.util.*
import javax.sql.DataSource

class GodkjenningLøsningRiver(
    val vedtaksperiodeId: UUID,
    val aktørId: String,
    val fødselsnummer: String,
    val periodetype: String,
    val inntektskilde: String,
    val godkjenning: Godkjenning,
    val utbetalingType: String,
    val refusjonType: String?
) {
    class Factory(rapid: RapidsConnection, private val dataSource: DataSource) : River.PacketListener {
        init {
            River(rapid).apply {
                validate {
                    it.demandAll("@behov", listOf("Godkjenning"))
                    it.rejectKey("@final")
                    it.require("@løsning.Godkjenning", ::tilGodkjenning)
                    it.requireKey(
                        "vedtaksperiodeId",
                        "aktørId",
                        "fødselsnummer",
                        "Godkjenning.periodetype",
                        "Godkjenning.inntektskilde",
                        "Godkjenning.utbetalingtype",
                        "@løsning.Godkjenning.godkjenttidspunkt",
                    )
                    it.interestedIn("@løsning.Godkjenning.refusjontype")
                }
            }.register(this)
        }

        companion object {
            val godkjentCounter = Counter.build(
                    "vedtaksperioder_godkjent",
                    "Antall godkjente vedtaksperioder"
            )
                    .register()

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

            val oppgaveTypeCounter = Counter.build(
                    "oppgavetype_behandlet",
                    "Antall oppgaver behandlet av type"
            )
                    .labelNames("type")
                    .register()
        }

        override fun onPacket(packet: JsonMessage, context: MessageContext) {
            if (godkjenningAlleredeLagret(packet)) return

            val løsning = GodkjenningLøsningRiver(
                vedtaksperiodeId = UUID.fromString(packet["vedtaksperiodeId"].asText()),
                fødselsnummer = packet["fødselsnummer"].asText(),
                aktørId = packet["aktørId"].asText(),
                periodetype = packet["Godkjenning.periodetype"].asText(),
                inntektskilde = packet["Godkjenning.inntektskilde"].asText(),
                utbetalingType = packet["Godkjenning.utbetalingtype"].asText(),
                refusjonType = packet["@løsning.Godkjenning.refusjontype"].takeIf { !it.isMissingOrNull() }?.asText(),
                godkjenning = tilGodkjenning(packet["@løsning.Godkjenning"])
            )

            log.info("Lagrer godkjenning for {}", keyValue("vedtaksperiodeId", løsning.vedtaksperiodeId))

            if (løsning.godkjenning.godkjent) {
                godkjentCounter.inc()
                oppgaveTypeCounter.labels(løsning.periodetype).inc()
            } else {
                if (løsning.godkjenning.årsak != null) {
                    årsakCounter.labels(løsning.godkjenning.årsak).inc()
                }
                løsning.godkjenning.begrunnelser?.forEach { begrunnelserCounter.labels(it).inc() }
            }

            dataSource.insertGodkjenning(løsning)
        }

        private fun godkjenningAlleredeLagret(packet: JsonMessage) =
            dataSource.godkjenningAlleredeLagret(
                UUID.fromString(packet["vedtaksperiodeId"].asText()),
                packet["@løsning.Godkjenning.godkjenttidspunkt"].asLocalDateTime()
            )

        private fun tilGodkjenning(jsonNode: JsonNode) = Godkjenning(
            godkjent = jsonNode["godkjent"].asBoolean(),
            saksbehandlerIdent = jsonNode["saksbehandlerIdent"].asText(),
            godkjentTidspunkt = jsonNode["godkjenttidspunkt"].asLocalDateTime(),
            årsak = jsonNode.optional("årsak")?.asText(),
            begrunnelser = jsonNode.optional("begrunnelser")?.map(JsonNode::asText),
            kommentar = jsonNode.optional("kommentar")?.asText()?.takeIf { it.isNotBlank() }
        )

        private fun JsonNode.optional(name: String) = takeIf { hasNonNull(name) }?.get(name)
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
