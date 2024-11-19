package no.nav.helse

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageProblems
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import net.logstash.logback.argument.StructuredArguments.kv
import no.nav.helse.LagtPåVent.Companion.lagreLagtPåVent
import no.nav.helse.LagtPåVent.Companion.parseLeggPåVent
import no.nav.helse.Util.jsonNode
import no.nav.helse.Util.withSessionAndReturnGeneratedKey
import javax.sql.DataSource

class LagtPåVentRiver(rapidsConnection: RapidsConnection, private val dataSource: DataSource): River.PacketListener {

    init {
        River(rapidsConnection).apply {
            precondition {
                it.requireValue("@event_name", "lagt_på_vent")
            }
            validate {
                it.requireKey("oppgaveId", "skalTildeles", "frist", "saksbehandlerIdent", "@opprettet", "årsaker", "saksbehandlerOid", "behandlingId")
                it.interestedIn("notatTekst")

            }
        }.register(this)
    }

    override fun onPreconditionError(error: MessageProblems, context: MessageContext, metadata: MessageMetadata) {
        sikkerlogg.error("Klarte ikke å lese lagt_på_vent event! ${error.toExtendedReport()}")
    }

    override fun onSevere(error: MessageProblems.MessageException, context: MessageContext) {
        sikkerlogg.error("Klarte ikke å lese lagt_på_vent event! ${error.printStackTrace()}")

    }

    override fun onPacket(
        packet: JsonMessage,
        context: MessageContext,
        metadata: MessageMetadata,
        meterRegistry: MeterRegistry
    ) {
        val jsonNode = packet.jsonNode()
        val lagtPåVent = jsonNode.parseLeggPåVent()
        dataSource.withSessionAndReturnGeneratedKey {
            this.lagreLagtPåVent(lagtPåVent)
        }
        sikkerlogg.info("Leser inn hendelse {}", kv("lagt_på_vent", packet.toJson()))
    }

    override fun onError(problems: MessageProblems, context: MessageContext, metadata: MessageMetadata) {
        sikkerlogg.error("Klarte ikke å lese lagt_på_vent event! ${problems.toExtendedReport()}")
    }
}