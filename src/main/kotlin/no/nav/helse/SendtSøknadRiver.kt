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
        val flexId = packet["id"].asUuid()
        val hendelseId = packet["@id"].asUuid()
        val kort_soknad = packet["@event_name"].asText() == "sendt_søknad_arbeidsgiver"
        insertSøknad(flexId, hendelseId, kort_soknad)
    }

    private fun insertSøknad(
        flexId: UUID,
        hendelseId: UUID,
        kort_soknad: Boolean
    ) {
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query =
                """INSERT INTO soknad(flexId, hendelseId, kort_soknad) VALUES(:flexId, :hendelseId, :kort_soknad)"""
            session.run(
                queryOf(
                    query, mapOf(
                        "flexId" to flexId,
                        "hendelseId" to hendelseId,
                        "kort_soknad" to kort_soknad
                    )
                ).asExecute
            )
        }
        log.info("Lagrer soknad med flexId $flexId og hendelseId $hendelseId")
    }
}
