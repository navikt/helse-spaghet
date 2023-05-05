package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.*
import org.intellij.lang.annotations.Language
import java.time.LocalDateTime
import java.util.*
import javax.sql.DataSource

class FunksjonellFeilRiver(
        rapidApplication: RapidsConnection,
        private val dataSource: DataSource
) : River.PacketListener {

    init {
        River(rapidApplication).apply {
            validate {
                it.demandValue("@event_name", "aktivitetslogg_ny_aktivitet")
                it.requireKey("@opprettet")
                it.requireArray("aktiviteter") {
                    requireKey("nivå", "melding", "varselkode")
                    requireArray("kontekster") {
                        requireKey("konteksttype", "kontekstmap")
                    }
                }
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val opprettet = packet["@opprettet"].asLocalDateTime()
        packet["aktiviteter"]
            .filter { it["nivå"].asText() == "FUNKSJONELL_FEIL" }
            .forEach { aktivitet ->
                val vedtaksperiodeId = aktivitet["kontekster"]
                    .firstOrNull { kontektst -> kontektst["konteksttype"].asText() == "Vedtaksperiode" }
                    ?.get("kontekstmap")
                    ?.get("vedtaksperiodeId")
                    ?.let { UUID.fromString(it.asText()) }
                    ?: return log.error("Fant ingen vedtaksperiodeId knyttet til funksjonell feil")

                val nivå = aktivitet.path("nivå").asText()
                val melding = aktivitet.path("melding").asText()
                val varselkode = aktivitet.path("varselkode").asText()
                insertFunksjonellFeil(vedtaksperiodeId, varselkode, nivå, melding, opprettet)
                log.info("Lagret funksjonell feil på vedtaksperiode $vedtaksperiodeId")
            }
    }

    private fun insertFunksjonellFeil(
        vedtaksperiodeId: UUID,
        varselkode: String,
        nivå: String,
        melding: String,
        opprettet: LocalDateTime
    ) {
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query =
                """INSERT INTO funksjonell_feil(vedtaksperiode_id, varselkode, nivå, melding, opprettet) VALUES(:vedtaksperiode_id, :varselkode, :nivaa, :melding, :opprettet)"""
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
    }
}
