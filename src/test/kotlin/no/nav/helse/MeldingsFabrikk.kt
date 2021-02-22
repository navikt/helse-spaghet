package no.nav.helse

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.intellij.lang.annotations.Language
import java.time.LocalDateTime
import java.util.*

private val objectMapper: ObjectMapper = jacksonObjectMapper()
    .registerModule(JavaTimeModule())
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

@Language("JSON")
fun behovNyttFormat(fødselsnummer: String, vedtaksperiodeId: UUID, periodetype: String, id: UUID = UUID.randomUUID(), inntektskilde: String = "EN_ARBEIDSGIVER") = """
        {
          "@event_name": "behov",
          "@opprettet": "2020-06-02T12:00:00.000000",
          "@id": "$id",
          "@behov": [
            "Godkjenning"
          ],
          "aktørId": "1000000000000",
          "fødselsnummer": "$fødselsnummer",
          "organisasjonsnummer": "987654321",
          "vedtaksperiodeId": "$vedtaksperiodeId",
          "tilstand": "AVVENTER_GODKJENNING",
          "Godkjenning": {
            "warnings": {
              "aktiviteter": [
                {
                  "kontekster": [],
                  "alvorlighetsgrad": "WARN",
                  "melding": "Perioden er en direkte overgang fra periode i Infotrygd",
                  "detaljer": {},
                  "tidsstempel": "2020-06-02 15:56:34.111"
                }
              ],
              "kontekster": [] 
            },
            "periodeFom": "2020-05-16",
            "periodeTom": "2020-05-22",
            "periodetype": "$periodetype",
            "inntektskilde": "$inntektskilde"  
          }
        }
    """


fun løsningNyttFormat(
        fødselsnummer: String,
        vedtaksperiodeId: UUID,
        periodetype: String,
        id: UUID = UUID.randomUUID(),
        automatiskBehandlet: Boolean = false,
        saksbehandlerIdent: String = "Z999999",
) =
    objectMapper.readValue<ObjectNode>(behovNyttFormat(fødselsnummer, vedtaksperiodeId, periodetype, id)).apply {
            set<ObjectNode>(
                "@løsning", objectMapper.readTree(""" {
                "Godkjenning": {
                    "godkjent": true,
                    "saksbehandlerIdent": "$saksbehandlerIdent",
                    "godkjenttidspunkt": "2020-06-02T13:00:00.000000",
                    "automatiskBehandling": $automatiskBehandlet,
                    "begrunnelser": [
                            "Arbeidsgiverperiode beregnet feil"
                    ]
                }
            }"""))
            .put("@besvart", "2020-06-02T13:00:00.000000")
    }.toString()

@Language("JSON")
fun vedtaksperiodeEndret(
        hendelseId: UUID = UUID.randomUUID(),
        vedtaksperiodeId: UUID = UUID.randomUUID(),
        orgnummer: String = "98765432",
        timestamp: LocalDateTime = LocalDateTime.now(),
        tilstandFra: String = "AVVENTER_GODKJENNING",
        tilstandTil: String = "AVVENTER_SIMULERING"
) = """
{
  "vedtaksperiodeId": "$vedtaksperiodeId",
  "organisasjonsnummer": "$orgnummer",
  "gjeldendeTilstand": "$tilstandFra",
  "forrigeTilstand": "$tilstandTil",
  "aktivitetslogg": {
    "aktiviteter": [
      {
        "kontekster": [0, 1],
        "alvorlighetsgrad": "INFO",
        "melding": "Behandler simulering",
        "detaljer": {},
        "tidsstempel": "$timestamp"
      },
      {
        "kontekster": [0, 1, 2, 3, 4],
        "alvorlighetsgrad": "INFO",
        "melding": "Simulering kom frem til et annet totalbeløp. Kontroller beløpet til utbetaling",
        "detaljer": {},
        "tidsstempel": "$timestamp"
      }
    ],
    "kontekster": [
      {
        "kontekstType": "Simulering",
        "kontekstMap": {
          "meldingsreferanseId": "eb288a68-f49f-4c24-bdc6-3fdef4e57630",
          "aktørId": "100000231823",
          "fødselsnummer": "12121212345",
          "organisasjonsnummer": "$orgnummer"
        }
      },
      {
        "kontekstType": "Person",
        "kontekstMap": {
          "fødselsnummer": "12121212345",
          "aktørId": "100000231823"
        }
      },
      {
        "kontekstType": "Arbeidsgiver",
        "kontekstMap": {
          "organisasjonsnummer": "$orgnummer"
        }
      },
      {
        "kontekstType": "Vedtaksperiode",
        "kontekstMap": {
          "vedtaksperiodeId": "$vedtaksperiodeId"
        }
      },
      {
        "kontekstType": "Tilstand",
        "kontekstMap": {
          "tilstand": "AVVENTER_SIMULERING"
        }
      }
    ]
  },
  "@event_name":"vedtaksperiode_endret",
  "@id": "$hendelseId",
  "@opprettet": "$timestamp",
  "@forårsaket_av": {
    "event_name": "behov",
    "id": "ae2a4ee4-304a-4482-879f-40b412880e17",
    "opprettet": "${timestamp.minusMinutes(3)}"
  },
  "aktørId": "100000231823",
  "fødselsnummer": "12121212345"
} 
"""
