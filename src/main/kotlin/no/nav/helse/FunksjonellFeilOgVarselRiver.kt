package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import kotliquery.queryOf
import kotliquery.sessionOf
import net.logstash.logback.argument.StructuredArguments.keyValue
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
            precondition { it.requireValue("@event_name", "aktivitetslogg_ny_aktivitet") }
            validate {
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
    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
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
                    "VARSEL" -> insert(vedtaksperiodeId, varselkode, nivå, melding, "regelverksvarsel", opprettet)
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
                """INSERT INTO $tabellNavn(vedtaksperiode_id, varselkode, nivaa, melding, opprettet) VALUES(:vedtaksperiode_id, :varselkode, :nivaa, :melding, :opprettet)"""
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
        logg.info("Lagret $tabellNavn på vedtaksperiode $vedtaksperiodeId")
    }
}
