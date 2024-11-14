package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers.isMissingOrNull
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import kotliquery.Session
import kotliquery.queryOf
import no.nav.helse.Util.withSession
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
            precondition { it.requireValue("@event_name", "hendelse_ikke_håndtert") }
            validate {
                it.requireKey("hendelseId", "@opprettet")
                it.interestedIn("årsaker")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        val hendelseId = UUID.fromString(packet["hendelseId"].asText())
        val opprettet = packet["@opprettet"].asLocalDateTime()
        val årsaker = packet["årsaker"].takeUnless(JsonNode::isMissingOrNull)
            ?.map { it.asText() } ?: emptyList()

        if (årsaker.isEmpty()) logg.warn("Mangler årsaker i hendelse_ikke_håndtert")

        dataSource.withSession {
            årsaker.forEach { årsak ->
                this.insertHendelseIkkeHåndtertÅrsak(hendelseId, opprettet, årsak)
            }
        }
        logg.info("Lagret hendelse_ikke_håndtert for hendelseId=${hendelseId}")
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
