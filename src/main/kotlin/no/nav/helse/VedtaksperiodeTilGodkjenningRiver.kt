package no.nav.helse

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import kotliquery.queryOf
import kotliquery.sessionOf
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
            precondition { it.requireAll("@behov", listOf("Godkjenning")) }
            validate {
                it.requireKey("@behovId", "@opprettet")
                it.interestedIn("vedtaksperiodeId")
                it.forbid("@løsning")
                it.forbid("@final")
            }

        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
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