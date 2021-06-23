package no.nav.helse

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.MessageProblems
import no.nav.helse.rapids_rivers.River
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.junit.jupiter.api.Assertions

object TestUtil {
    fun TestRapid.failOnExceptions() {
        River(this).apply {
            validate {}
        }.register(object : River.PacketListener {
            override fun onPacket(packet: JsonMessage, context: MessageContext) {}

            override fun onSevere(error: MessageProblems.MessageException, context: MessageContext): Unit =
                Assertions.fail<Unit>("Caught severe from rapid", error)

            override fun onError(problems: MessageProblems, context: MessageContext): Unit =
                Assertions.fail<Unit>("Caught problems from rapid: $problems")
        })
    }
}