package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import kotliquery.queryOf
import kotliquery.sessionOf
import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.helse.rapids_rivers.*
import org.intellij.lang.annotations.Language
import java.time.LocalDateTime
import java.util.*
import javax.sql.DataSource

class FunksjonellFeilOgVarselRiver(
        rapidApplication: RapidsConnection,
        private val dataSource: DataSource
) : River.PacketListener {
    init {
        River(rapidApplication).apply {
            validate {
                it.demandValue("@event_name", "aktivitetslogg_ny_aktivitet")
                it.requireKey("@opprettet")
                it.requireArray("aktiviteter") {
                    requireKey("nivå", "melding")
                    interestedIn("varselkode")
                    requireArray("kontekster") {
                        requireKey("konteksttype", "kontekstmap")
                    }
                }
            }
        }.register(this)
    }
    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        sikkerlogg.info("Leser inn {}", keyValue("hendelse", packet.toJson()))
        val opprettet = packet["@opprettet"].asLocalDateTime()
        packet["aktiviteter"]
            .filter { it["nivå"].asText() in listOf("FUNKSJONELL_FEIL", "VARSEL") }
            .distinctBy { Triple(it["nivå"].asText(), it.finnVedtaksperiodeId(), it["varselkode"].asText()) }
            .forEach { aktivitet ->
                val vedtaksperiodeId = aktivitet
                    .finnVedtaksperiodeId()
                    ?: return@forEach sikkerlogg.info("Fant ingen vedtaksperiodeId knyttet til funksjonell feil på {}", keyValue("hendelse", packet.toJson()))

                val nivå = aktivitet.path("nivå").asText()
                val melding = aktivitet.path("melding").asText()
                val varselkode = aktivitet.path("varselkode").asText()
                when (nivå) {
                    "FUNKSJONELL_FEIL" -> insert(vedtaksperiodeId, varselkode, nivå, melding,"funksjonell_feil", opprettet)
                    "VARSEL" -> insert(vedtaksperiodeId, varselkode, nivå, melding, "varsel", opprettet)
                }
            }
    }

    private fun JsonNode.finnVedtaksperiodeId() = this["kontekster"]
        .firstOrNull { kontektst -> kontektst["konteksttype"].asText() == "Vedtaksperiode" }
            ?.get("kontekstmap")
            ?.get("vedtaksperiodeId")
            ?.let { UUID.fromString(it.asText()) }

    private fun insert(
        vedtaksperiodeId: UUID,
        varselkode: String,
        nivå: String,
        melding: String,
        tabellNavn: String,
        opprettet: LocalDateTime
    ) {
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query =
                """INSERT INTO $tabellNavn(vedtaksperiode_id, varselkode, nivå, melding, opprettet) VALUES(:vedtaksperiode_id, :varselkode, :nivaa, :melding, :opprettet)"""
            session.run(
                queryOf(
                    query, mapOf(
                        "vedtaksperiode_id" to vedtaksperiodeId,
                        "varselkode" to varselkode,
                        "nivaa" to nivå,
                        "melding" to melding,
                        "opprettet" to opprettet,
                    )
                ).asExecute
            )
        }
        log.info("Lagret $tabellNavn på vedtaksperiode $vedtaksperiodeId")
    }
}
