package no.nav.helse

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers.test_support.TestRapid
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageProblems
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