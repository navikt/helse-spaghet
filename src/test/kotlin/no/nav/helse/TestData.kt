package no.nav.helse

import no.nav.helse.AktivitetRiver.Nivå
import no.nav.helse.AktivitetRiver.Nivå.FUNKSJONELL_FEIL
import no.nav.helse.AktivitetRiver.Nivå.INFO
import no.nav.helse.TestData.Aktivitet.Companion.toJson
import no.nav.helse.Util.toJson
import org.intellij.lang.annotations.Language
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.UUID.randomUUID
import kotlin.random.Random.Default.nextLong

object TestData {
    fun Annullering.toJson(): String =
        """{
            "@event_name": "annullering",
            "saksbehandler": {"oid": "$saksbehandler"},
            "vedtaksperiodeId": "$vedtaksperiodeId",
            "begrunnelser": ${begrunnelser.toJson()},
            ${kommentar?.let { """"kommentar": "$it",""" } ?: ""}
            "@opprettet": "$opprettet",
            "arsaker": ${arsaker?.map { arsak ->
            """
                {"arsak": "${arsak.arsak}", "key": "${arsak.key}"}
            """
            }}
         }""".trimMargin()

    val annullering = Annullering(
        saksbehandler = randomUUID(),
        vedtaksperiodeId = randomUUID(),
        begrunnelser = listOf(),
        kommentar = null,
        opprettet = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS),
        arsaker = emptyList()
    )

    fun Annullering.vedtaksperiodeId(it: UUID) = copy(vedtaksperiodeId = it)
    fun Annullering.kommentar(it: String) = copy(kommentar = it)
    fun Annullering.begrunnelse(it: String) = copy(begrunnelser = begrunnelser + it)
    fun Annullering.årsak(it: AnnulleringArsak) = copy(arsaker = arsaker?.plus(it))

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

    data class NyOppgave(
        val id: Long = nextLong(),
        val egenskaper: List<String> = listOf("SØKNAD", "EN_ARBEIDSGIVER", "UTBETALING_TIL_SYKMELDT"),
        val fødselsnummer: String = "12345678910",
        val tilstand: String = "AvventerSaksbehandler",
    ) {
        fun id(id: Long) = copy(id = id)
        fun egenskaper(vararg egenskaper: String) = copy(egenskaper = egenskaper.toList())
        fun fødselsnummer(fødselsnummer: String) = copy(fødselsnummer = fødselsnummer)
        fun tilstand(tilstand: String) = copy(tilstand = tilstand)

        @Language("JSON")
        fun toJson() = """
        {
          "@event_name": "oppgave_oppdatert",
          "hendelseId": "698bbf15-af2c-40e9-9e6c-0e93b1a30b61",
          "oppgaveId": $id,
          "tilstand": "$tilstand",
          "fødselsnummer": "$fødselsnummer",
          "egenskaper": [
            ${egenskaper.joinToString { """"$it"""" }}
          ],
          "beslutter": {
            "epostadresse": "beslutter@nav.no",
            "oid": "5ba9ce8b-b834-4109-866c-e14d35a28d74"
          },
          "saksbehandler": {
            "epostadresse": "saksbehandler@nav.no",
            "oid": "3208409d-d6e7-442b-a010-d2b14bd8bfbe"
          },
          "@id": "0bfd3123-e9e7-4519-8f8c-d750b576aa99",
          "@opprettet": "2024-01-23T12:16:16.944417"
        }
        """.trimIndent()
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
        fun error() = copy(alvorlighetsgrad = FUNKSJONELL_FEIL.name)
        fun info() = copy(alvorlighetsgrad = INFO.name, varselkode = null)
        fun toJson() = """{
            "melding" : "$melding",
            "nivå" : "$alvorlighetsgrad",
            ${if (varselkode != null) """ "varselkode": "$varselkode", """ else """"""}
            "tidsstempel" : "$tidsstempel",
            "kontekster": ${
            kontekster.map { kontekst ->
                """{
                    "konteksttype": "${kontekst.getValue("konteksttype")}",
                    "kontekstmap": {
                    ${
                    (kontekst.getValue("kontekstmap") as Map<String, String>).entries.joinToString {
                        """
                           "${it.key}": "${it.value}" 
                        """
                    }
                }
                    }
                }
                """
            }
        }
           }
        """.trimIndent()

        fun melding(it: String) = copy(melding = it)
        fun vedtaksperiodeId(id: UUID) = copy(
            kontekster = kontekster + listOf(
                mapOf(
                    "konteksttype" to "Vedtaksperiode",
                    "kontekstmap" to mapOf(
                        "vedtaksperiodeId" to id.toString()
                    )
                )
            )
        )

        companion object {
            fun List<Aktivitet>.toJson(vedtaksperiodeId: UUID): String =
                joinToString(separator = ", ", prefix = "[", postfix = "]") {
                    it.vedtaksperiodeId(vedtaksperiodeId).toJson()
                }
        }
    }

    val vedtaksperiodeEndret = VedtaksperiodeEndret()
    val nyAktivitet = NyAktivitet()
    val nyOppgave = NyOppgave()
    val aktivitet = Aktivitet()
}
