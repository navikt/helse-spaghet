package no.nav.helse

import no.nav.helse.AktivitetRiver.Nivå
import no.nav.helse.AktivitetRiver.Nivå.FUNKSJONELL_FEIL
import no.nav.helse.AktivitetRiver.Nivå.INFO
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
            "@opprettet": "$opprettet"
         }""".trimMargin()

    val annullering = Annullering(
        saksbehandler = randomUUID(),
        fagsystemId = "ABCD12345",
        begrunnelser = listOf(),
        kommentar = null,
        opprettet = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS),
    )

    fun Annullering.fagsystemId(it: String) = copy(fagsystemId = it)
    fun Annullering.kommentar(it: String) = copy(kommentar = it)
    fun Annullering.begrunnelse(it: String) = copy(begrunnelser = begrunnelser + it)

    data class VedtaksperiodeEndret(
        val vedtaksperiodeId: UUID = randomUUID(),
        val aktiviteter: List<Aktivitet> = listOf(),
        val meldingsId: UUID = randomUUID(),
        val opprettet: LocalDateTime = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS),
        val gjeldendeTilstand: String = "TIL_INFOTRYGD",
        val forrigeTilstand: String = "AVVENTER_GAP",
        val forårsaketAv: UUID = randomUUID(),
        val kildeType: String? = null
    ) {
        fun forrigeTilstand(it: String) = copy(forrigeTilstand = it)
        fun gjeldendeTilstand(it: String) = copy(gjeldendeTilstand = it)
        fun kildeType(it: String) = copy(kildeType = it)
        fun toJson() =
            """{
            "@event_name": "vedtaksperiode_endret",
            "vedtaksperiodeId": "$vedtaksperiodeId",
            "gjeldendeTilstand": "$gjeldendeTilstand",
            "forrigeTilstand": "$forrigeTilstand",
            "@opprettet": "$opprettet",
            "@id": "$meldingsId",
            "@forårsaket_av": {"id": "$forårsaketAv", "event_name": "$kildeType"}
         }""".trimMargin()


    }
    data class NyAktivitet(
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
        fun kontekstType(it: String) = copy(kontekstType = it)
        fun toJson() =
            """{
            "@event_name": "aktivitetslogg_ny_aktivitet",
            "aktiviteter": ${aktiviteter.toJson(vedtaksperiodeId)},
            "@opprettet": "$opprettet",
            "@id": "$meldingsId",
            "@forårsaket_av": {"id": "$forårsaketAv"}
         }""".trimMargin()
    }

    data class Aktivitet(
        val melding: String = "Uffda, dette ble rart",
        val alvorlighetsgrad: String = Nivå.VARSEL.name,
        val varselkode: String? = "RV_IV_1",
        val tidsstempel: LocalDateTime = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS),
        val kontekster: List<Map<String, Any>> = emptyList()
    ) {
        fun error() = copy (alvorlighetsgrad = FUNKSJONELL_FEIL.name)
        fun info()  = copy (alvorlighetsgrad = INFO.name, varselkode = null)
        fun toJson() = """{
            "melding" : "$melding",
            "nivå" : "$alvorlighetsgrad",
            ${ if (varselkode != null) """ "varselkode": "$varselkode", """ else """"""}
            "tidsstempel" : "$tidsstempel",
            "kontekster": ${kontekster.map { kontekst ->
                """{
                    "konteksttype": "${kontekst.getValue("konteksttype")}",
                    "kontekstmap": {
                    ${(kontekst.getValue("kontekstmap") as Map<String, String>).entries.joinToString { 
                        """
                           "${it.key}": "${it.value}" 
                        """
                }}
                    }
                }
                """
        }}
           }
        """.trimIndent()

        fun melding(it: String) = copy(melding = it)
        fun vedtaksperiodeId(id: UUID) = copy(kontekster = kontekster + listOf(
            mapOf(
                "konteksttype" to "Vedtaksperiode",
                "kontekstmap" to mapOf(
                    "vedtaksperiodeId" to id.toString()
                )
            )
        ))

        companion object {
            fun List<Aktivitet>.toJson(vedtaksperiodeId: UUID): String =
                joinToString(separator = ", ", prefix = "[", postfix = "]") { it.vedtaksperiodeId(vedtaksperiodeId).toJson() }
        }
    }
    val vedtaksperiodeEndret = VedtaksperiodeEndret()
    val nyAktivitet = NyAktivitet()
    val aktivitet = Aktivitet()
}
