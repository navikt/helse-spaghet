package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.intellij.lang.annotations.Language
import java.util.*

private val objectMapper: ObjectMapper = jacksonObjectMapper()
    .registerModule(JavaTimeModule())
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

@Language("JSON")
fun nyBehov(fødselsnummer: String, vedtaksperiodeId: UUID, periodetype: String, id: UUID = UUID.randomUUID()) = """
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
            "periodetype": "$periodetype"
          }
        }
    """


fun nyLøsning(fødselsnummer: String, vedtaksperiodeId: UUID, periodetype: String, id: UUID = UUID.randomUUID()) =
    objectMapper.readValue<ObjectNode>(nyBehov(fødselsnummer, vedtaksperiodeId, periodetype, id)).apply {
            set<ObjectNode>(
                "@løsning", objectMapper.readTree(""" {
                "Godkjenning": {
                "godkjent": true,
                "saksbehandlerIdent": "Z999999",
                "godkjenttidspunkt": "2020-06-02T13:00:00.000000",
                "begrunnelser": [
                        "Arbeidsgiverperiode beregnet feil"
                    ]
            }
            }"""))
            .set<ObjectNode>("@final", objectMapper.valueToTree(true))
            .set<JsonNode>("@besvart", objectMapper.valueToTree("2020-06-02T13:00:00.000000"))
    }.toString()


@Language("JSON")
fun gammelBehov(fødselsnummer: String, vedtaksperiodeId: UUID, periodetype: String?, id: UUID = UUID.randomUUID()) = """
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
          "periodeFom": "2020-05-16",
          "periodeTom": "2020-05-22",
          "sykepengegrunnlag": 42069.0,
          ${periodetype?.let { "\"periodetype\": \"$it\"," } ?: ""}
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
          }
        }
    """


fun gammelLøsning(fødselsnummer: String, vedtaksperiodeId: UUID, periodetype: String?, id: UUID = UUID.randomUUID()) =
    objectMapper.readValue<ObjectNode>(gammelBehov(fødselsnummer, vedtaksperiodeId, periodetype, id)).apply {
        set<ObjectNode>(
            "@løsning", objectMapper.readTree("""{ "Godkjenning": {
              "godkjent": true,
              "saksbehandlerIdent": "Z999999",
              "godkjenttidspunkt": "2020-06-02T13:00:00.000000",
              "begrunnelser": [
                    "Arbeidsgiverperiode beregnet feil"
                ]
            }
          }"""
            ))
            .set<ObjectNode>("@final", objectMapper.valueToTree(true))
            .set<JsonNode>("@besvart", objectMapper.valueToTree("2020-06-02T13:00:00.000000"))
    }.toString()