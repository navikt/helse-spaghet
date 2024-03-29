package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import no.nav.helse.rapids_rivers.asLocalDateTime
import org.intellij.lang.annotations.Language
import java.time.LocalDateTime
import java.util.*
import javax.sql.DataSource

class VedtaksperiodeTilGodkjenningRiver(
    rapidApplication: RapidsConnection,
    private val dataSource: DataSource
) : River.PacketListener {
    init {
        River(rapidApplication).apply {
            validate {
                it.demandAll("@behov", listOf("Godkjenning"))
                it.requireKey("@behovId", "@opprettet")
                it.interestedIn("vedtaksperiodeId")
                it.forbid("@løsning")
                it.forbid("@final")
            }

        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val json = objectMapper.readTree(packet.toJson())
        val vedtaksperiodeId = UUID.fromString(json["vedtaksperiodeId"].asText())
        val behovOpprettet = json["@opprettet"].asLocalDateTime()
        val periodetype = json["Godkjenning"]["periodetype"].asText()
        val inntektskilde = json["Godkjenning"]["inntektskilde"].asText()
        val id = UUID.fromString(json["@behovId"].asText())
        insertGodkjenningsbehov(id, periodetype, inntektskilde, vedtaksperiodeId, behovOpprettet)
        logg.info("Lagret godkjenningsbehov for vedtaksperiodeId=$vedtaksperiodeId")
    }

    private fun insertGodkjenningsbehov(
        id: UUID,
        periodetype: String?,
        inntektskilde: String,
        vedtaksperiodeId: UUID,
        tidspunkt: LocalDateTime
    ) {
        @Language("PostgreSQL")
        val insertGodkjenningsbehov =
            "INSERT INTO godkjenningsbehov(id, vedtaksperiode_id, periodetype, inntektskilde, tidspunkt) VALUES(:id, :vedtaksperiode_id, :periodetype, :inntektskilde, :tidspunkt) ON CONFLICT DO NOTHING;"
        sessionOf(dataSource).use { session ->
            session.run(
                queryOf(
                    insertGodkjenningsbehov, mapOf(
                        "id" to id,
                        "vedtaksperiode_id" to vedtaksperiodeId,
                        "periodetype" to periodetype,
                        "inntektskilde" to inntektskilde,
                        "tidspunkt" to tidspunkt
                    )
                ).asUpdate
            )
        }
    }
}