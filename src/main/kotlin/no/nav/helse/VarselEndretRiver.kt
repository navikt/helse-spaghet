package no.nav.helse

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import kotliquery.Session
import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.Util.asUuid
import org.intellij.lang.annotations.Language
import java.time.LocalDateTime
import java.util.*
import javax.sql.DataSource

/**
 * Leser informasjon om endring i varslers status
 */
class VarselEndretRiver(
    rapidApplication: RapidsConnection,
    private val dataSource: DataSource
) : River.PacketListener {
    init {
        River(rapidApplication).apply {
            validate {
                it.demandValue("@event_name", "varsel_endret")
                it.requireKey(
                    "@id", "@opprettet",
                    "varseltittel", "varselkode", "gjeldende_status",
                    "varsel_id", "vedtaksperiode_id"
                )
                it.interestedIn("behandling_id")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val vedtaksperiodeId = UUID.fromString(packet["vedtaksperiode_id"].asText())
        val tittel = packet["varseltittel"].asText()
        val meldingOpprettet = packet["@opprettet"].asLocalDateTime()
        val status = packet["gjeldende_status"].asText()
        val behandlingId = packet["behandling_id"].textValue()?.let { UUID.fromString(it) }
        val varselId = packet["varsel_id"].asUuid()
        val varselkode = packet["varselkode"].asText()
        val kilde = with(varselkode) {
            when {
                startsWith("SB") -> "Spesialist"
                startsWith("RV") -> "Spleis"
                else -> throw IllegalStateException("Støtter ikke varselkode: $this")
            }
        }
        val varsel = Varsel(
            id = varselId,
            vedtaksperiodeId = vedtaksperiodeId,
            kode = varselkode,
            melding = tittel,
            kilde = kilde,
            status = status
        )

        sessionOf(dataSource).use { session ->
            session.insertVarsel(varsel, meldingOpprettet, behandlingId)
            logg.info("Lagret varsel for behandlingId=$behandlingId, vedtaksperiode=$vedtaksperiodeId")
        }
    }

    data class Varsel(
        val id: UUID,
        val vedtaksperiodeId: UUID,
        val kode: String,
        val melding: String,
        val kilde: String,
        val status: String
    )

    private companion object {
        private fun Session.insertVarsel(varsel: Varsel, sistEndret: LocalDateTime, behandlingId: UUID?): Int {
            @Language("PostgreSQL")
            val statement = """
                INSERT INTO varsel(varsel_id, behandling_id, vedtaksperiode_id, varselkode, kilde, tittel, status, sist_endret, godkjenning_varsel_id)
                VALUES (:varselId, :behandlingId, :vedtaksperiodeId, :varselkode, :kilde, :tittel, :status, :sistEndret, null)
                ON CONFLICT (varsel_id) DO NOTHING;
            """

            return run(
                queryOf(
                    statement, mapOf(
                        "varselId" to varsel.id,
                        "behandlingId" to behandlingId,
                        "varselkode" to varsel.kode,
                        "kilde" to varsel.kilde,
                        "tittel" to varsel.melding,
                        "status" to varsel.status,
                        "vedtaksperiodeId" to varsel.vedtaksperiodeId,
                        "sistEndret" to sistEndret
                    )
                ).asUpdate
            )
        }
    }
}
