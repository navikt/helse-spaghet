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
                it.demandAny("@event_name", listOf("sendt_søknad_nav", "sendt_søknad_arbeidsgiver"))
                it.requireKey("id", "@id")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val dokumentId = packet["id"].asUuid()
        val hendelseId = packet["@id"].asUuid()
        val kortSoknad = packet["@event_name"].asText() == "sendt_søknad_arbeidsgiver"
        insertSøknad(dokumentId, hendelseId, kortSoknad)
    }

    private fun insertSøknad(
        dokumentId: UUID,
        hendelseId: UUID,
        kortSoknad: Boolean
    ) {
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query =
                """INSERT INTO soknad(dokument_id, hendelse_id, kort_soknad) VALUES(:dokumentId, :hendelseId, :kortSoknad)"""
            session.run(
                queryOf(
                    query, mapOf(
                        "dokumentId" to dokumentId,
                        "hendelseId" to hendelseId,
                        "kortSoknad" to kortSoknad
                    )
                ).asExecute
            )
        }
        log.info("Lagrer soknad med dokumentId $dokumentId og hendelseId $hendelseId")
    }
}
