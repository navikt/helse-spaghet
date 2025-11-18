package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers.isMissingOrNull
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import com.github.navikt.tbd_libs.result_object.getOrThrow
import com.github.navikt.tbd_libs.retry.retryBlocking
import com.github.navikt.tbd_libs.speed.SpeedClient
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import net.logstash.logback.argument.StructuredArguments.kv
import no.nav.helse.RefusjonstypeTag.Arbeidsgiverutbetaling
import no.nav.helse.RefusjonstypeTag.IngenUtbetaling
import no.nav.helse.RefusjonstypeTag.Personutbetaling
import java.util.*
import javax.sql.DataSource

private enum class RefusjonstypeTag {
    Arbeidsgiverutbetaling,
    NegativArbeidsgiverutbetaling,
    Personutbetaling,
    NegativPersonutbetaling,
    IngenUtbetaling,
}

class GodkjenningLøsningRiver(
    rapid: RapidsConnection,
    private val dataSource: DataSource,
    private val speedClient: SpeedClient,
) : River.PacketListener {
    val godkjentCounter =
        Counter
            .builder("vedtaksperioder_godkjent")
            .description("Antall godkjente vedtaksperioder")
            .register(meterRegistry)

    init {
        River(rapid)
            .apply {
                precondition {
                    it.requireAll("@behov", listOf("Godkjenning"))
                    it.forbid("@final")
                }
                validate {
                    it.require("@løsning.Godkjenning", ::tilLøsning)
                    it.requireKey(
                        "vedtaksperiodeId",
                        "fødselsnummer",
                        "Godkjenning.periodetype",
                        "Godkjenning.inntektskilde",
                        "Godkjenning.utbetalingtype",
                        "Godkjenning.behandlingId",
                        "Godkjenning.tags",
                        "@løsning.Godkjenning.godkjenttidspunkt",
                    )
                    it.interestedIn(
                        "@løsning.Godkjenning.saksbehandleroverstyringer",
                    )
                }
            }.register(this)
    }

    override fun onPacket(
        packet: JsonMessage,
        context: MessageContext,
        metadata: MessageMetadata,
        meterRegistry: MeterRegistry,
    ) {
        if (godkjenningAlleredeLagret(packet)) return

        val ident = packet["fødselsnummer"].asText()
        val behandlingId = UUID.fromString(packet["Godkjenning.behandlingId"].asText())

        val refusjonstypeTags =
            packet["Godkjenning.tags"].mapNotNull {
                runCatching {
                    enumValueOf<RefusjonstypeTag>(it.asText())
                }.getOrNull()
            }

        if (refusjonstypeTags.isEmpty()) error("Mangler tag for refusjonstype")

        val refusjonstype =
            when {
                IngenUtbetaling in refusjonstypeTags -> "INGEN_UTBETALING"
                Arbeidsgiverutbetaling in refusjonstypeTags && Personutbetaling in refusjonstypeTags -> "DELVIS_REFUSJON"
                Arbeidsgiverutbetaling !in refusjonstypeTags && Personutbetaling in refusjonstypeTags -> "INGEN_REFUSJON"
                Arbeidsgiverutbetaling in refusjonstypeTags && Personutbetaling !in refusjonstypeTags -> "FULL_REFUSJON"
                else -> "NEGATIVT_BELØP"
            }

        val identer = retryBlocking { speedClient.hentFødselsnummerOgAktørId(ident, behandlingId.toString()).getOrThrow() }

        val behov =
            Godkjenningsbehov(
                vedtaksperiodeId = UUID.fromString(packet["vedtaksperiodeId"].asText()),
                fødselsnummer = identer.fødselsnummer,
                aktørId = identer.aktørId,
                periodetype = packet["Godkjenning.periodetype"].asText(),
                inntektskilde = packet["Godkjenning.inntektskilde"].asText(),
                utbetalingType = packet["Godkjenning.utbetalingtype"].asText(),
                refusjonType = refusjonstype,
                saksbehandleroverstyringer =
                    packet["@løsning.Godkjenning.saksbehandleroverstyringer"]
                        .takeUnless(JsonNode::isMissingOrNull)
                        ?.map {
                            UUID.fromString(it.asText())
                        } ?: emptyList(),
                løsning = tilLøsning(packet["@løsning.Godkjenning"]),
                behandlingId = behandlingId,
            )

        logg.info("Lagrer godkjenning for {}", kv("vedtaksperiodeId", behov.vedtaksperiodeId))

        if (behov.løsning.godkjent) {
            godkjentCounter.increment()
            Counter
                .builder("oppgavetype_behandlet")
                .description("Antall oppgaver behandlet av type")
                .tag("type", behov.periodetype)
                .register(meterRegistry)
                .increment()
        } else {
            if (behov.løsning.årsak != null) {
                Counter
                    .builder("vedtaksperioder_avvist_arsak")
                    .description("Antall avviste vedtaksperioder")
                    .tag("arsak", behov.løsning.årsak)
                    .register(meterRegistry)
                    .increment()
            }
            behov.løsning.begrunnelser?.forEach {
                Counter
                    .builder("vedtaksperioder_avvist_begrunnelser")
                    .description("Antall avviste vedtaksperioder")
                    .tag("begrunnelse", it)
                    .register(meterRegistry)
                    .increment()
            }
        }

        dataSource.insertGodkjenning(behov)
    }

    private fun godkjenningAlleredeLagret(packet: JsonMessage) =
        dataSource.godkjenningAlleredeLagret(
            UUID.fromString(packet["vedtaksperiodeId"].asText()),
            packet["@løsning.Godkjenning.godkjenttidspunkt"].asLocalDateTime(),
        )

    private fun tilLøsning(jsonNode: JsonNode) =
        Godkjenningsbehov.Løsning(
            godkjent = jsonNode["godkjent"].asBoolean(),
            saksbehandlerIdent = jsonNode["saksbehandlerIdent"].asText(),
            godkjentTidspunkt = jsonNode["godkjenttidspunkt"].asLocalDateTime(),
            årsak = jsonNode.getIfNotNull("årsak")?.asText(),
            begrunnelser = jsonNode.getIfNotNull("begrunnelser")?.map(JsonNode::asText),
            kommentar = jsonNode.getIfNotNull("kommentar")?.asText()?.takeIf { it.isNotBlank() },
        )

    private fun JsonNode.getIfNotNull(name: String) = takeIf { hasNonNull(name) }?.get(name)
}
