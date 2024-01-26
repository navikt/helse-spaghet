package no.nav.helse.ventetilstand

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import org.slf4j.event.Level

internal object Slack {
    internal fun MessageContext.sendPåSlack(packet: JsonMessage, level: Level, melding: String, channel: String? = null) {
        val map = mutableMapOf(
            "melding" to "$melding\n\n - Deres erbødig SPaghet :spaghet:",
            "level" to level.name,
            "system_participating_services" to packet["system_participating_services"],
        ).apply {
            channel?.let { put("channel", it) }
        }
        val slackmelding = JsonMessage.newMessage("slackmelding", map).toJson()
        publish(slackmelding)
    }
}