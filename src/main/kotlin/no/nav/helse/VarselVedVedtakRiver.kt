package no.nav.helse

import java.lang.RuntimeException
import kotliquery.Session
import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import org.intellij.lang.annotations.Language
import java.util.*
import javax.sql.DataSource

/**
 * Leser informasjon fra events som Spesialist sender ut samtidig som den svarer på godkjenningsbehov,
 * for å ta vare på warnings både fra Spleis og Spesialist.
 */
class VarselVedVedtakRiver(
    rapidApplication: RapidsConnection,
    private val dataSource: DataSource
) : River.PacketListener {
    init {
        River(rapidApplication).apply {
            validate {
                it.demandValue("@event_name", "varsel_endret")
                it.requireKey("@id", "vedtaksperiode_id_til_godkjenning", "varseltittel", "varselkode")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val json = objectMapper.readTree(packet.toJson())
        val vedtaksperiodeId = UUID.fromString(json["vedtaksperiode_id_til_godkjenning"].asText())
        val tittel = json["varseltittel"].asText()
        val kilde = with(json["varselkode"].asText()) {
            when {
                startsWith("SB") -> "Spesialist"
                startsWith("RV") -> "Spleis"
                else -> throw IllegalStateException("Støtter ikke varselkode: $this")
            }
        }
        val varsel = Varsel(tittel, kilde)

        sessionOf(dataSource).use { session ->
            val godkjenningId = session.findGodkjenningId(vedtaksperiodeId)
                ?: throw RuntimeException("Forventet godkjenning for vedtaksperiode $vedtaksperiodeId")

            session.insertVarsel(godkjenningId, varsel)
            logg.info("Lagret varsel for godkjenningId=$godkjenningId, vedtaksperiode=$vedtaksperiodeId")
        }
    }

    data class Varsel(
        val melding: String,
        val kilde: String
    )

    private companion object {
        private fun Session.insertVarsel(godkjenningId: Int, varsel: Varsel): Int {
            @Language("PostgreSQL")
            val statement = """
                INSERT INTO warning_for_godkjenning(godkjenning_ref, melding, kilde)
                VALUES (:godkjenningId, :melding, :kilde)
                ON CONFLICT (godkjenning_ref, melding) DO NOTHING;
            """

            return run(
                queryOf(
                    statement, mapOf(
                        "godkjenningId" to godkjenningId,
                        "melding" to varsel.melding,
                        "kilde" to varsel.kilde
                    )
                ).asUpdate
            )
        }
    }
}
