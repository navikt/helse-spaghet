package no.nav.helse

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageProblems
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import kotliquery.queryOf
import kotliquery.sessionOf
import org.intellij.lang.annotations.Language
import java.time.LocalDateTime
import java.util.*
import javax.sql.DataSource

class UtkastTilVedtakRiver(
    rapidApplication: RapidsConnection,
    private val dataSource: DataSource
) : River.PacketListener {
    init {
        River(rapidApplication).apply {
            precondition { it.requireValue("@event_name", "utkast_til_vedtak") }
            validate {
                it.requireKey("@opprettet", "@id", "tags", "behandlingId", "vedtaksperiodeId")
                it.interestedIn("vedtaksperiodeId")
            }

        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        val vedtaksperiodeId = UUID.fromString(packet["vedtaksperiodeId"].asText())
        val behandlingId = UUID.fromString(packet["behandlingId"].asText())
        val opprettet = packet["@opprettet"].asLocalDateTime()
        val id = UUID.fromString(packet["@id"].asText())
        val tags = packet["tags"].map { it.asText() }
        insertUtkastTilVedtak(id, opprettet, vedtaksperiodeId, behandlingId, tags)
        logg.info("Lagret utkast_til_vedtak for vedtaksperiodeId=$vedtaksperiodeId")
    }

    override fun onError(problems: MessageProblems, context: MessageContext, metadata: MessageMetadata) {
        sikkerlogg.error("Klarte ikke å lese utkast_til_vedtak event! ${problems.toExtendedReport()}")
    }

    private fun insertUtkastTilVedtak(
        id: UUID,
        opprettet: LocalDateTime,
        vedtaksperiodeId: UUID,
        behandlingId: UUID,
        tags: List<String>
    ) {
        @Language("PostgreSQL")
        val insertUtkastTilVedtak =
            "INSERT INTO utkast_til_vedtak(id, opprettet, vedtaksperiode_id, behandling_id, tags) VALUES(:id, :opprettet, :vedtaksperiode_id, :behandling_id, :tags::varchar[]) ON CONFLICT DO NOTHING;"
        sessionOf(dataSource).use { session ->
            session.run(
                queryOf(
                    insertUtkastTilVedtak, mapOf(
                        "id" to id,
                        "opprettet" to opprettet,
                        "vedtaksperiode_id" to vedtaksperiodeId,
                        "behandling_id" to behandlingId,
                        "tags" to tags.joinToString(prefix = "{", postfix = "}")
                    )
                ).asUpdate
            )
        }
    }
}