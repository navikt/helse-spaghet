package no.nav.helse

import no.nav.helse.Annullering.Companion.insertAnnullering
import no.nav.helse.Annullering.Companion.parseAnnullering
import no.nav.helse.Util.jsonNode
import no.nav.helse.Util.withSession
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import javax.sql.DataSource

class AnnulleringRiver(
    rapidApplication: RapidsConnection,
    private val dataSource: DataSource
) : River.PacketListener {
    init {
        River(rapidApplication).apply {
            validate {
                it.demandValue("@event_name", "annullering")
                it.requireKey("saksbehandler.oid", "fagsystemId", "begrunnelser", "gjelderSisteSkjæringstidspunkt", "@opprettet")
                it.interestedIn("kommentar")
            }
        }.register(this)
    }


    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val annullering = packet.jsonNode().parseAnnullering()
        dataSource.withSession {
            this.insertAnnullering(annullering)
        }
        log.info("Lagret annullering for fagsystemId=${annullering.fagsystemId}")
    }
}
