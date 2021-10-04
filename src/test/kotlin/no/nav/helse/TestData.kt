package no.nav.helse

import no.nav.helse.AktivitetRiver.Companion.legacyDateFormat
import no.nav.helse.TestData.Aktivitet.Companion.toJson
import no.nav.helse.Util.toJson
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.UUID.randomUUID

object TestData {
    fun Annullering.toJson(): String =
        """{
            "@event_name": "annullering",
            "saksbehandler": {"oid": "$saksbehandler"},
            "fagsystemId": "$fagsystemId",
            "begrunnelser": ${begrunnelser.toJson()},
            ${kommentar?.let {""""kommentar": "$it",""" } ?: ""}
            "gjelderSisteSkjæringstidspunkt": $gjelderSisteSkjæringstidspunkt,
            "@opprettet": "$opprettet"
         }""".trimMargin()

    val annullering = Annullering(
        saksbehandler = randomUUID(),
        fagsystemId = "ABCD12345",
        begrunnelser = listOf(),
        kommentar = null,
        opprettet = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS),
        gjelderSisteSkjæringstidspunkt = true
    )

    fun Annullering.fagsystemId(it: String) = copy(fagsystemId = it)
    fun Annullering.kommentar(it: String) = copy(kommentar = it)
    fun Annullering.begrunnelse(it: String) = copy(begrunnelser = begrunnelser + it)
    fun Annullering.gjelderSisteSkjæringstidspunkt(it: Boolean) = copy(gjelderSisteSkjæringstidspunkt = it)

    data class VedtaksperiodeEndret(
        val vedtaksperiodeId: UUID = randomUUID(),
        val aktiviteter: List<Aktivitet> = listOf(),
        val meldingsId: UUID = randomUUID(),
        val opprettet: LocalDateTime = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS),
        val gjeldendeTilstand: String = "TIL_INFOTRYGD",
        val forrigeTilstand: String = "AVVENTER_GAP",
        val forårsaketAv: UUID = randomUUID(),
        val kontekstType: String? = null
    ) {
        fun aktivitet(it: Aktivitet) = copy(aktiviteter = aktiviteter + it)
        fun forrigeTilstand(it: String) = copy(forrigeTilstand = it)
        fun gjeldendeTilstand(it: String) = copy(gjeldendeTilstand = it)
        fun kontekstType(it: String) = copy(kontekstType = it)
        fun toJson() =
            """{
            "@event_name": "vedtaksperiode_endret",
            "aktivitetslogg": {
            "aktiviteter": ${aktiviteter.toJson()}
            ${kontekstType?.let { """, "kontekster": [{"kontekstType": "$kontekstType"}]""" } ?: ""}
            },
            "vedtaksperiodeId": "$vedtaksperiodeId",
            "gjeldendeTilstand": "$gjeldendeTilstand",
            "forrigeTilstand": "$forrigeTilstand",
            "@opprettet": "$opprettet",
            "@id": "$meldingsId",
            "@forårsaket_av": {"id": "$forårsaketAv"}
         }""".trimMargin()


    }

    data class Aktivitet(
        val melding: String = "Uffda, dette ble rart",
        val alvorlighetsgrad: String = "WARNING",
        val tidsstempel: LocalDateTime = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS),
    ) {
        fun error() = copy (alvorlighetsgrad = "ERROR")
        fun info()  = copy (alvorlighetsgrad = "INFO")
        fun toJson() = """{
            "melding" : "$melding",
            "alvorlighetsgrad" : "$alvorlighetsgrad",
            "tidsstempel" : "${tidsstempel.format(legacyDateFormat)}"
           }
        """.trimIndent()

        fun melding(it: String) = copy(melding = it)

        companion object {
            fun List<Aktivitet>.toJson(): String = map{it.toJson()}.joinToString ( separator = ", ", prefix = "[", postfix = "]")
        }
    }

    val vedtaksperiodeEndret = VedtaksperiodeEndret()
    val aktivitet = Aktivitet()
}
