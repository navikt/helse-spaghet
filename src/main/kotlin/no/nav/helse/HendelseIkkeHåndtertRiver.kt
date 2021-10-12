package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import kotliquery.Session
import kotliquery.queryOf
import no.nav.helse.Util.withSession
import no.nav.helse.rapids_rivers.*
import org.intellij.lang.annotations.Language
import java.time.LocalDateTime
import java.util.*
import javax.sql.DataSource

class HendelseIkkeHåndtertRiver(
    rapidApplication: RapidsConnection,
    private val dataSource: DataSource
) : River.PacketListener {
    init {
        River(rapidApplication).apply {
            validate {
                it.demandValue("@event_name", "hendelse_ikke_håndtert")
                it.requireKey("hendelseId", "@opprettet")
                it.interestedIn("årsaker")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val hendelseId = UUID.fromString(packet["hendelseId"].asText())
        val opprettet = packet["@opprettet"].asLocalDateTime()
        val årsaker = packet["årsaker"].takeUnless(JsonNode::isMissingOrNull)
            ?.map { it.asText() } ?: emptyList()

        if (årsaker.isEmpty()) log.warn("Mangler årsaker i hendelse_ikke_håndtert")

        dataSource.withSession {
            årsaker.forEach { årsak ->
                this.insertHendelseIkkeHåndtertÅrsak(hendelseId, opprettet, årsak)
            }
        }
        log.info("Lagret hendelse_ikke_håndtert for hendelseId=${hendelseId}")
    }

    fun Session.insertHendelseIkkeHåndtertÅrsak(hendelseId: UUID, opprettet: LocalDateTime, årsak: String) {
        @Language("PostgreSQL")
        val statement = """
                INSERT INTO hendelse_ikke_håndtert_årsak(
                    hendelse_id,
                    tidsstempel,
                    årsak
                ) VALUES ( ?, ?, ?)
                ON CONFLICT (hendelse_id, årsak) DO NOTHING;
            """
        run(
            queryOf(
                statement,
                hendelseId,
                opprettet,
                årsak,
            ).asUpdate
        )
    }
}
