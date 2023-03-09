package no.nav.helse.ventetilstand

import com.fasterxml.jackson.databind.JsonNode
import no.nav.helse.rapids_rivers.*
import java.util.*
import javax.sql.DataSource

class VedtaksperiodeVenterRiver (
    rapidApplication: RapidsConnection,
    private val dataSource: DataSource
    ) : River.PacketListener {

    init {
        River(rapidApplication).apply {
            validate { it.requireValue("@event_name", "vedtaksperiode_venter") }
            validate { it.requireKey(
                "venterPå.venteårsak.hva",
                "venterPå.vedtaksperiodeId",
                "venterPå.organisasjonsnummer"
            ) }
            validate { it.interestedIn("venterPå.venteårsak.hvorfor") }
            validate { it.requireKey(
                "@id",
                "vedtaksperiodeId",
                "organisasjonsnummer",
                "ventetSiden",
                "venterTil",
                "fødselsnummer"
            ) }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        TODO("Not yet implemented")
    }
}