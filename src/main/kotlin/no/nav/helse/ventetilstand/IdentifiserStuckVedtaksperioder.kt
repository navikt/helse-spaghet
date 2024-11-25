package no.nav.helse.ventetilstand

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import com.github.navikt.tbd_libs.spurtedu.SkjulRequest
import com.github.navikt.tbd_libs.spurtedu.SpurteDuClient
import io.micrometer.core.instrument.MeterRegistry
import no.nav.helse.objectMapper
import no.nav.helse.ventetilstand.Slack.sendPåSlack
import org.slf4j.LoggerFactory
import org.slf4j.event.Level.ERROR
import org.slf4j.event.Level.INFO
import java.time.DayOfWeek.*
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.time.temporal.ChronoUnit.DAYS
import java.util.UUID
import kotlin.time.*

internal class IdentifiserStuckVedtaksperioder(
    rapidsConnection: RapidsConnection,
    private val dao: VedtaksperiodeVentetilstandDao,
    private val spurteDuClient: SpurteDuClient
): River.PacketListener {

    init {
        River(rapidsConnection).apply {
            precondition { it.requireValue("@event_name", "identifiser_stuck_vedtaksperioder") }
            validate {
                it.requireKey("system_participating_services")
            }
        }.register(this)
        River(rapidsConnection).apply {
            precondition {
                it.requireValue("@event_name", "halv_time")
                it.require("time") { time ->
                    check(time.asInt() in setOf(8, 13))
                }
                it.requireValue("minutt", 30)
                it.forbidValues("ukedag", listOf("SATURDAY", "SUNDAY"))
            }
            validate {
                it.requireKey("system_participating_services")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        try {
            val (stuck, tidsbruk) = measureTimedValue { dao.stuck() }
            if (stuck.isEmpty()) return ingentingStuck(packet, context, tidsbruk)

            val ventetLengstPerPerson = stuck
                .groupBy { it.vedtaksperiodeVenter.fødselsnummer }
                .mapValues { (_, vedtaksperiodeVenterMedMetadata) -> vedtaksperiodeVenterMedMetadata.minBy { it.vedtaksperiodeVenter.ventetSiden } }
                .entries
                .sortedBy { (_, vedtaksperiodeVenterMedMetadata) -> vedtaksperiodeVenterMedMetadata.vedtaksperiodeVenter.ventetSiden }
                .map { (fødselsnummer, vedtaksperiodeVenterMedMetadata) -> fødselsnummer to vedtaksperiodeVenterMedMetadata }
                .takeUnless { it.isEmpty() } ?: return ingentingStuck(packet, context, tidsbruk)

            val antall = ventetLengstPerPerson.size

            var melding =
                "\n\nBrukte ${tidsbruk.snygg} på å finne ut at det er ${stuck.size.vedtaksperioder} som ser ut til å være stuck! :helene-redteam:\n" +
                "Fordelt på ${antall.personer}:\n\n"

            melding += ventetLengstPerPerson.take(Maks).joinToString(separator = "\n") { (fnr, vedtaksperiodeVenterMedMetadata) ->
                val vedtaksperiodeVenter = vedtaksperiodeVenterMedMetadata.vedtaksperiodeVenter
                val venterPå = vedtaksperiodeVenter.venterPå
                "\t${vedtaksperiodeVenterMedMetadata.prefix} venter på ${venterPå.snygg} ${venterPå.suffix(fnr, venterPå.vedtaksperiodeId, spurteDuClient)}"
            }

            if (antall > Maks) melding += "\n\t... og ${antall - Maks} til.. :melting_face:"

            context.sendPåSlack(packet, ERROR, melding)
        } catch (exception: Exception) {
            sikkerlogg.error("Feil ved identifisering av stuck vedtaksperioder", exception)
        }
    }

    private companion object {
        private val Maks = 50
        private val sikkerlogg = LoggerFactory.getLogger("tjenestekall")
        private val VenterPå.snygg get() = (if (hvorfor == null) hva else "$hva fordi $hvorfor").lowercase().replace("_", " ")

        private val VedtaksperiodeVenterMedMetadata.prefix get() = "${vedtaksperiodeVenter.venterPå.vedtaksperiodeId}".take(5).uppercase().let {
            val venteklassifisering = venteklassifisering(registrert = tidsstempel)
            when (venteklassifisering) {
                Venteklassifisering.VANLIG -> "*$it*"
                Venteklassifisering.GAMMEL -> "*$it* :pepe-freezing:"
                Venteklassifisering.NYHET -> "*$it* :news:"
            }
        }
        private fun VenterPå.suffix(fnr: String, vedtaksperiodeId: UUID, spurteDuClient: SpurteDuClient) = "[${spurteDuClient.spannerUrl(fnr, vedtaksperiodeId, "Spanner (utvikler)", tbdgruppeProd)}/${spurteDuClient.spannerUrl(fnr, vedtaksperiodeId, "Spanner (saksbehandler)", tbdSpannerProd)}/${kibanaUrl}]"

        private val JsonMessage.eventname get() = get("@event_name").asText()
        private fun ingentingStuck(packet: JsonMessage, context: MessageContext, tidsbruk: Duration) {
            if (packet.eventname == "identifiser_stuck_vedtaksperioder") return context.sendPåSlack(packet, INFO, "\n\nBrukte ${tidsbruk.snygg} på å finne ut at du kan bare ta det helt :musical_keyboard:! Ingenting er stuck! Gå tilbake til det du egentlig skulle gjøre :heart:")
            sikkerlogg.info("Ingen vedtaksperioder er stuck per ${LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS)}")
        }

        private val Int.personer get() = if (this == 1) "én person" else "$this personer"
        private val Int.vedtaksperioder get() = if (this == 1) "én vedtaksperiode" else "$this vedtaksperioder"
        private fun menneskete(ting: String, antall: Int, prefix: String = "") = if (antall <= 0) "" else if (antall == 1) "$prefix$antall $ting" else "$prefix$antall ${ting}er"
        private val Duration.snygg get() = toJavaDuration().let { when {
            it.seconds == 0L -> "under ett sekund"
            it.seconds < 60L -> menneskete("sekund", it.seconds.toInt())
            else -> "${menneskete("minutt", it.toMinutesPart())} ${menneskete("sekund", it.toSecondsPart(), prefix = "og ")}"
        }}

        private const val tbdgruppeProd = "c0227409-2085-4eb2-b487-c4ba270986a3"
        private const val tbdSpannerProd = "382f42f4-f46b-40c1-849b-38d6b5a1f639"

        private fun SpurteDuClient.spannerUrl(fnr: String, vedtaksperiodeId: UUID, linktext: String, tilgang: String): String {
            val payload = SkjulRequest.SkjulTekstRequest(
                tekst = objectMapper.writeValueAsString(mapOf(
                    "ident" to fnr,
                    "identtype" to "FNR"
                )),
                påkrevdTilgang = tilgang
            )

            val spurteDuLink = skjul(payload)
            val spannerLink = "https://spanner.ansatt.nav.no/person/${spurteDuLink.id}?vedtaksperiodeId=$vedtaksperiodeId"
            return "<$spannerLink|$linktext>"
        }

        private val VenterPå.kibanaUrl get() = "https://logs.adeo.no/app/kibana#/discover?_a=(index:'tjenestekall-*',query:(language:lucene,query:'%22${vedtaksperiodeId}%22'))&_g=(time:(from:'${LocalDateTime.now().minusDays(1)}',mode:absolute,to:now))".let { url ->
            "<$url|Kibana>"
        }
    }
}

internal enum class Venteklassifisering { VANLIG, NYHET, GAMMEL }
internal fun venteklassifisering(registrert: LocalDateTime, nå: LocalDateTime = LocalDateTime.now()): Venteklassifisering {
    if (DAYS.between(registrert, nå) > 5) return Venteklassifisering.GAMMEL
    val nyhet = when (nå.dayOfWeek) {
        SUNDAY -> nå.minusHours(48) // Drar med oss lørdan' på søndan'
        MONDAY -> nå.minusHours(72) // Drar med oss lørdan' og søndan' på mandan'
        else -> nå.minusHours(24)
    }
    if (registrert >= nyhet) return Venteklassifisering.NYHET
    return Venteklassifisering.VANLIG
}