package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.*
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
            validate {
                it.demandValue("@event_name", "utkast_til_vedtak")
                it.requireKey("@opprettet", "@id", "tags", "behandlingId", "vedtaksperiodeId")
                it.interestedIn("vedtaksperiodeId")
            }

        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val vedtaksperiodeId = UUID.fromString(packet["vedtaksperiodeId"].asText())
        val behandlingId = UUID.fromString(packet["behandlingId"].asText())
        val opprettet = packet["@opprettet"].asLocalDateTime()
        val id = UUID.fromString(packet["@id"].asText())
        val tags = packet["tags"].map { it.asText() }
        insertUtkastTilVedtak(id, opprettet, vedtaksperiodeId, behandlingId, tags)
        logg.info("Lagret utkast_til_vedtak for vedtaksperiodeId=$vedtaksperiodeId")
    }

    override fun onError(problems: MessageProblems, context: MessageContext) {
        super.onError(problems, context)
    }

    override fun onSevere(error: MessageProblems.MessageException, context: MessageContext) {
        super.onSevere(error, context)
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
            "INSERT INTO utkast_til_vedtak(id, opprettet, vedtaksperiode_id, behandlingId, tags) VALUES(:id, :opprettet, :vedtaksperiode_id, :behandling_id, :tags) ON CONFLICT DO NOTHING;"
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