package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import no.nav.helse.rapids_rivers.asLocalDateTime
import java.time.LocalDateTime
import java.util.UUID
import javax.sql.DataSource

class GodkjenningLøsning(
    val vedtaksperiodeId: UUID,
    val aktørId: String,
    val fødselsnummer: String,
    val warnings: List<String>,
    val godkjenning: Godkjenning
) {
    class Factory(rapid: RapidsConnection, private val dataSource: DataSource) : River.PacketListener {
        init {
            River(rapid).apply {
                validate {
                    it.demandAll("@behov", listOf("Godkjenning"))
                    it.demandValue("@final", true)
                    it.requireKey("@løsning")
                    it.interestedIn("warnings", "vedtaksperiodeId", "aktørId", "fødselsnummer")
                }
            }.register(this)
        }

        override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
            val løsning = GodkjenningLøsning(
                vedtaksperiodeId = UUID.fromString(packet["vedtaksperiodeId"].asText()),
                fødselsnummer = packet["fødselsnummer"].asText(),
                aktørId = packet["aktørId"].asText(),
                warnings = packet["warnings"].warnings(),
                godkjenning = packet["@løsning"]["Godkjenning"].let {
                    Godkjenning(
                        godkjent = it["godkjent"].asBoolean(),
                        saksbehandlerIdent = it["saksbehandlerIdent"].asText(),
                        godkjentTidspunkt = it["godkjenttidspunkt"].asLocalDateTime(),
                        årsak = it.optional("årsak")?.asText(),
                        begrunnelser = it.optional("begrunnelser")?.map(JsonNode::asText),
                        kommentar = it.optional("kommentar")?.asText()
                    )
                }
            )

            dataSource.insertGodkjenning(løsning)
        }

        private fun JsonNode.optional(name: String) = takeIf { hasNonNull(name) }?.get(name)

        private fun JsonNode.warnings() = this["aktiviteter"].map { it["melding"].asText() }
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
