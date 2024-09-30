package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.*
import org.postgresql.util.PSQLException
import java.util.*
import javax.sql.DataSource

class SkatteinntekterLagtTilGrunnRiver(rapidsConnection: RapidsConnection, private val dataSource: DataSource) : River.PacketListener {



    init {
        River(rapidsConnection).apply {
            validate {
                it.demandValue("@event_name", "skatteinntekter_lagt_til_grunn")
                it.requireKey("vedtaksperiodeId", "behandlingId", "organisasjonsnummer", "fødselsnummer", "@id")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        sikkerlogg.info("Leste melding: ${packet.toJson()}")
        val vedtaksperiodeId = packet["vedtaksperiodeId"].asText().let { UUID.fromString(it) }
        val behandlingId = packet["behandlingId"].asText().let { UUID.fromString(it) }
        val hendelseId = packet["@id"].asText().let { UUID.fromString(it) }
        val orgnummer = packet["organisasjonsnummer"].asText()
        val fnr = packet["fødselsnummer"].asText()

        try {
            sessionOf(dataSource).use { session ->
                val insert = """
                    INSERT INTO skatteinntekter_lagt_til_grunn(vedtaksperiode_id, behandling_id, hendelse_id, fnr, orgnummer, data)
                    VALUES(:vedtaksperiode_id, :behandling_id, :hendelse_id, :fnr, :orgnummer, :data::jsonb)
                    """

                session.update(queryOf(insert, mapOf(
                    "vedtaksperiode_id" to vedtaksperiodeId,
                    "behandling_id" to behandlingId,
                    "hendelse_id" to hendelseId,
                    "fnr" to fnr,
                    "orgnummer" to orgnummer,
                    "data" to packet.toJson()
                )))
            }
        } catch (error: PSQLException) {
            logg.error("Klarte ikke lagre skatteinntekter lagt til grunn for hendelseId $hendelseId")
        }
    }

    override fun onError(problems: MessageProblems, context: MessageContext) {
        sikkerlogg.error(problems.toExtendedReport())
    }
}
