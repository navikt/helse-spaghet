package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.Util.asUuid
import no.nav.helse.rapids_rivers.*
import org.intellij.lang.annotations.Language
import java.util.*
import javax.sql.DataSource

class SendtSøknadRiver(
        rapidApplication: RapidsConnection,
        private val dataSource: DataSource
) : River.PacketListener {

    init {
        River(rapidApplication).apply {
            validate {
                it.demandAny("@event_name", listOf(
                    "sendt_søknad_nav",
                    "sendt_søknad_arbeidsgiver",
                    "sendt_søknad_selvstendig",
                    "sendt_søknad_frilanser",
                    "sendt_søknad_arbeidsledig")
                )
                it.requireKey("id", "@id", "type", "arbeidssituasjon")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val dokumentId = packet["id"].asUuid()
        val hendelseId = packet["@id"].asUuid()
        val eventName = packet["@event_name"].asText()
        val soknadstype = packet["type"].asText()
        val arbeidssituasjon = packet["arbeidssituasjon"].asText()
        insertSøknad(dokumentId, hendelseId, eventName, soknadstype, arbeidssituasjon)
    }

    private fun insertSøknad(
        dokumentId: UUID,
        hendelseId: UUID,
        eventName: String,
        soknadstype: String,
        arbeidssituasjon: String
    ) {
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query =
                """INSERT INTO soknad(dokument_id, hendelse_id, event, soknadstype, arbeidssituasjon) VALUES(:dokumentId, :hendelseId, :event, :soknadstype, :arbeidssituasjon)"""
            session.run(
                queryOf(
                    query, mapOf(
                        "dokumentId" to dokumentId,
                        "hendelseId" to hendelseId,
                        "event" to eventName,
                        "soknadstype" to soknadstype,
                        "arbeidssituasjon" to arbeidssituasjon
                    )
                ).asExecute
            )
        }
        logg.info("Lagrer søknad med dokumentId $dokumentId og hendelseId $hendelseId")
    }
}
