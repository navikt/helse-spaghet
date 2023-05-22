package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import io.prometheus.client.Counter
import net.logstash.logback.argument.StructuredArguments.kv
import no.nav.helse.rapids_rivers.*
import java.util.*
import javax.sql.DataSource

class GodkjenningLøsningRiver(rapid: RapidsConnection, private val dataSource: DataSource): River.PacketListener {
    init {
        River(rapid).apply {
            validate {
                it.demandAll("@behov", listOf("Godkjenning"))
                it.rejectKey("@final")
                it.require("@løsning.Godkjenning", ::tilLøsning)
                it.requireKey(
                    "vedtaksperiodeId",
                    "aktørId",
                    "fødselsnummer",
                    "Godkjenning.periodetype",
                    "Godkjenning.inntektskilde",
                    "Godkjenning.utbetalingtype",
                    "@løsning.Godkjenning.godkjenttidspunkt",
                )
                it.interestedIn(
                    "@løsning.Godkjenning.refusjontype",
                    "@løsning.Godkjenning.saksbehandleroverstyringer"
                )
            }
        }.register(this)
    }

    companion object {
        val godkjentCounter: Counter = Counter.build(
            "vedtaksperioder_godkjent",
            "Antall godkjente vedtaksperioder"
        )
                .register()

        val årsakCounter: Counter = Counter.build(
            "vedtaksperioder_avvist_arsak",
            "Antall avviste vedtaksperioder"
        )
                .labelNames("arsak")
                .register()

        val begrunnelserCounter: Counter = Counter.build(
            "vedtaksperioder_avvist_begrunnelser",
            "Antall avviste vedtaksperioder"
        )
                .labelNames("begrunnelse")
                .register()

        val oppgaveTypeCounter: Counter = Counter.build(
            "oppgavetype_behandlet",
            "Antall oppgaver behandlet av type"
        )
                .labelNames("type")
                .register()
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        if (godkjenningAlleredeLagret(packet)) return

        val behov = Godkjenningsbehov(
            vedtaksperiodeId = UUID.fromString(packet["vedtaksperiodeId"].asText()),
            fødselsnummer = packet["fødselsnummer"].asText(),
            aktørId = packet["aktørId"].asText(),
            periodetype = packet["Godkjenning.periodetype"].asText(),
            inntektskilde = packet["Godkjenning.inntektskilde"].asText(),
            utbetalingType = packet["Godkjenning.utbetalingtype"].asText(),
            refusjonType = packet["@løsning.Godkjenning.refusjontype"].takeIf { !it.isMissingOrNull() }?.asText(),
            saksbehandleroverstyringer = packet["@løsning.Godkjenning.saksbehandleroverstyringer"].takeUnless(JsonNode::isMissingOrNull)
                ?.map {
                    UUID.fromString(it.asText())
                } ?: emptyList(),
            løsning = tilLøsning(packet["@løsning.Godkjenning"])
        )

        logg.info("Lagrer godkjenning for {}", kv("vedtaksperiodeId", behov.vedtaksperiodeId))

        if (behov.løsning.godkjent) {
            godkjentCounter.inc()
            oppgaveTypeCounter.labels(behov.periodetype).inc()
        } else {
            if (behov.løsning.årsak != null) {
                årsakCounter.labels(behov.løsning.årsak).inc()
            }
            behov.løsning.begrunnelser?.forEach { begrunnelserCounter.labels(it).inc() }
        }

        dataSource.insertGodkjenning(behov)
    }

    private fun godkjenningAlleredeLagret(packet: JsonMessage) =
        dataSource.godkjenningAlleredeLagret(
            UUID.fromString(packet["vedtaksperiodeId"].asText()),
            packet["@løsning.Godkjenning.godkjenttidspunkt"].asLocalDateTime()
        )

    private fun tilLøsning(jsonNode: JsonNode) = Godkjenningsbehov.Løsning(
        godkjent = jsonNode["godkjent"].asBoolean(),
        saksbehandlerIdent = jsonNode["saksbehandlerIdent"].asText(),
        godkjentTidspunkt = jsonNode["godkjenttidspunkt"].asLocalDateTime(),
        årsak = jsonNode.optional("årsak")?.asText(),
        begrunnelser = jsonNode.optional("begrunnelser")?.map(JsonNode::asText),
        kommentar = jsonNode.optional("kommentar")?.asText()?.takeIf { it.isNotBlank() }
    )

    private fun JsonNode.optional(name: String) = takeIf { hasNonNull(name) }?.get(name)
}