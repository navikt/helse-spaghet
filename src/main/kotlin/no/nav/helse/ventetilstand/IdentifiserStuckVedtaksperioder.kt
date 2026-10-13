package no.nav.helse.ventetilstand

import com.github.navikt.tbd_libs.personpseudoid.Identitetsnummer
import com.github.navikt.tbd_libs.personpseudoid.PersonPseudoIdProvider
import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import no.nav.helse.ventetilstand.Slack.sendPåSlack
import no.nav.sykepenger.libs.logging.loggError
import no.nav.sykepenger.libs.logging.loggInfo
import org.slf4j.event.Level.ERROR
import org.slf4j.event.Level.INFO
import java.time.DayOfWeek.MONDAY
import java.time.DayOfWeek.SUNDAY
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.time.temporal.ChronoUnit.DAYS
import java.util.*
import kotlin.time.Duration
import kotlin.time.measureTimedValue
import kotlin.time.toJavaDuration

internal class IdentifiserStuckVedtaksperioder(
    rapidsConnection: RapidsConnection,
    private val dao: VedtaksperiodeVentetilstandDao,
    private val personPseudoIdProvider: PersonPseudoIdProvider,
) : River.PacketListener {
    init {
        River(rapidsConnection)
            .apply {
                precondition { it.requireValue("@event_name", "identifiser_stuck_vedtaksperioder") }
                validate {
                    it.requireKey("system_participating_services")
                }
            }.register(this)
        River(rapidsConnection)
            .apply {
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

    override fun onPacket(
        packet: JsonMessage,
        context: MessageContext,
        metadata: MessageMetadata,
        meterRegistry: MeterRegistry,
    ) {
        try {
            val (stuck, tidsbruk) = measureTimedValue { dao.stuck() }
            if (stuck.isEmpty()) return ingentingStuck(packet, context, tidsbruk)

            val ventetLengstPerPerson =
                stuck
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

            melding +=
                ventetLengstPerPerson.take(Maks).joinToString(separator = "\n") { (fnr, vedtaksperiodeVenterMedMetadata) ->
                    val vedtaksperiodeVenter = vedtaksperiodeVenterMedMetadata.vedtaksperiodeVenter
                    val venterPå = vedtaksperiodeVenter.venterPå
                    "\t${vedtaksperiodeVenterMedMetadata.prefix} venter på ${venterPå.snygg} ${venterPå.suffix(fnr, venterPå.vedtaksperiodeId, personPseudoIdProvider)}"
                }

            if (antall > Maks) melding += "\n\t... og ${antall - Maks} til.. :melting_face:"

            context.sendPåSlack(packet, ERROR, melding)
        } catch (exception: Exception) {
            loggError("Feil ved identifisering av stuck vedtaksperioder", exception)
        }
    }

    private companion object {
        private val Maks = 50
        private val VenterPå.snygg get() = (if (hvorfor == null) hva else "$hva fordi $hvorfor").lowercase().replace("_", " ")

        private val VedtaksperiodeVenterMedMetadata.prefix get() =
            "${vedtaksperiodeVenter.venterPå.vedtaksperiodeId}".take(5).uppercase().let {
                val venteklassifisering = venteklassifisering(registrert = tidsstempel)
                when (venteklassifisering) {
                    Venteklassifisering.VANLIG -> "*$it*"
                    Venteklassifisering.GAMMEL -> "*$it* :pepe-freezing:"
                    Venteklassifisering.NYHET -> "*$it* :news:"
                }
            }

        private fun VenterPå.suffix(
            fnr: String,
            vedtaksperiodeId: UUID,
            personPseudoIdProvider: PersonPseudoIdProvider,
        ) = "[${personPseudoIdProvider.spannerUrl(fnr, vedtaksperiodeId)}/$kibanaUrl]"

        private val JsonMessage.eventname get() = get("@event_name").asString()

        private fun ingentingStuck(
            packet: JsonMessage,
            context: MessageContext,
            tidsbruk: Duration,
        ) {
            if (packet.eventname == "identifiser_stuck_vedtaksperioder") return context.sendPåSlack(packet, INFO, "\n\nBrukte ${tidsbruk.snygg} på å finne ut at du kan bare ta det helt :musical_keyboard:! Ingenting er stuck! Gå tilbake til det du egentlig skulle gjøre :heart:")
            loggInfo("Ingen vedtaksperioder er stuck", "tidspunkt" to LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS).toString())
        }

        private val Int.personer get() = if (this == 1) "én person" else "$this personer"
        private val Int.vedtaksperioder get() = if (this == 1) "én vedtaksperiode" else "$this vedtaksperioder"

        private fun menneskete(
            ting: String,
            antall: Int,
            prefix: String = "",
        ) = if (antall <= 0) {
            ""
        } else if (antall == 1) {
            "$prefix$antall $ting"
        } else {
            "$prefix$antall ${ting}er"
        }

        private val Duration.snygg get() =
            toJavaDuration().let {
                when {
                    it.seconds == 0L -> "under ett sekund"
                    it.seconds < 60L -> menneskete("sekund", it.seconds.toInt())
                    else -> "${menneskete("minutt", it.toMinutesPart())} ${menneskete("sekund", it.toSecondsPart(), prefix = "og ")}"
                }
            }

        private fun PersonPseudoIdProvider.spannerUrl(
            fnr: String,
            vedtaksperiodeId: UUID,
        ): String {
            val personPseudoId = nyPersonPseudoId(Identitetsnummer(fnr))
            val spannerLink = "https://spanner.ansatt.nav.no/person/$personPseudoId?vedtaksperiodeId=$vedtaksperiodeId"
            return "<$spannerLink|Spanner>"
        }

        private val VenterPå.kibanaUrl get() =
            "https://console.cloud.google.com/logs/query;query=resource.labels.container_name:%22spleis%22%0AjsonPayload.message:%22$vedtaksperiodeId%22;duration=P1D?project=tbd-prod-eacd".let { url ->
                "<$url|Google Logging>"
            }
    }
}

internal enum class Venteklassifisering { VANLIG, NYHET, GAMMEL }

internal fun venteklassifisering(
    registrert: LocalDateTime,
    nå: LocalDateTime = LocalDateTime.now(),
): Venteklassifisering {
    if (DAYS.between(registrert, nå) > 5) return Venteklassifisering.GAMMEL
    val nyhet =
        when (nå.dayOfWeek) {
            SUNDAY -> nå.minusHours(48) // Drar med oss lørdan' på søndan'
            MONDAY -> nå.minusHours(72) // Drar med oss lørdan' og søndan' på mandan'
            else -> nå.minusHours(24)
        }
    if (registrert >= nyhet) return Venteklassifisering.NYHET
    return Venteklassifisering.VANLIG
}
