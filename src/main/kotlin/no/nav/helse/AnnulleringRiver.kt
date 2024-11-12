package no.nav.helse

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import no.nav.helse.Annullering.Companion.insertAnnullering
import no.nav.helse.Annullering.Companion.parseAnnullering
import no.nav.helse.Util.asUuid
import no.nav.helse.Util.jsonNode
import no.nav.helse.Util.withSession
import javax.sql.DataSource

class AnnulleringRiver(
    rapidApplication: RapidsConnection,
    private val dataSource: DataSource
) : River.PacketListener {
    init {
        River(rapidApplication).apply {
            validate {
                it.demandValue("@event_name", "annullering")
                it.requireKey("vedtaksperiodeId", "begrunnelser", "@opprettet")
                it.require("saksbehandler.oid") { node -> node.asUuid() }
                it.interestedIn("kommentar")
                it.interestedIn("arsaker")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {

        val jsonNode = packet.jsonNode()
        val annullering = jsonNode.parseAnnullering()
        dataSource.withSession {
            this.insertAnnullering(annullering)
        }
        logg.info("Lagret annullering for vedtaksperiodeId=${annullering.vedtaksperiodeId}")
    }
}
