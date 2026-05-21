package no.nav.helse

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageProblems
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import no.nav.helse.AnmodningOmForkasting.Companion.insertAnmodningOmForkasting
import no.nav.helse.AnmodningOmForkasting.Companion.parseAnmodningOmForkasting
import no.nav.helse.Util.jsonNode
import no.nav.helse.Util.withSession
import javax.sql.DataSource

class AnmodningOmForkastingRiver(
    rapidsConnection: RapidsConnection,
    private val dataSource: DataSource,
) : River.PacketListener {
    init {
        River(rapidsConnection)
            .apply {
                precondition { it.requireValue("@event_name", "anmodning_om_forkasting") }
                validate {
                    it.requireKey("vedtaksperiodeId", "fødselsnummer", "organisasjonsnummer", "yrkesaktivitetstype", "årsaker", "@opprettet")
                    it.interestedIn("saksbehandlerIdent", "@avsender", "kommentar")
                }
            }.register(this)
    }

    override fun onError(
        problems: MessageProblems,
        context: MessageContext,
        metadata: MessageMetadata,
    ) {
        sikkerlogg.error("Klarte ikke å lese AnmodningOmForkasting event! ${problems.toExtendedReport()}")
    }

    override fun onPacket(
        packet: JsonMessage,
        context: MessageContext,
        metadata: MessageMetadata,
        meterRegistry: MeterRegistry,
    ) {
        val jsonNode = packet.jsonNode()
        val anmodning = jsonNode.parseAnmodningOmForkasting() ?: run {
            logg.warn("Forkaster anmodning_om_forkasting: verken saksbehandlerIdent eller @avsender.NAVIdent er tilstede")
            return
        }
        dataSource.withSession {
            this.insertAnmodningOmForkasting(anmodning)
        }
        logg.info("Lagret anmodning_om_forkasting for vedtaksperiodeId=${anmodning.vedtaksperiodeId}")
    }
}
