package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.*
import org.postgresql.util.PSQLException
import java.util.*
import javax.sql.DataSource

class VedtaksperiodeDumpRiver(
    rapidApplication: RapidsConnection,
    private val dataSource: DataSource
) : River.PacketListener {
    init {
        River(rapidApplication).apply {
            validate {
                it.demandValue("@event_name", "vedtaksperiode_data")
                it.requireKey(
                    "aktørId",
                    "fødselsnummer",
                    "yrkesaktivitet",
                    "fom",
                    "tom",
                    "skjæringstidspunkt",
                    "tilstand",
                    "oppdatert",
                    "vedtaksperiodeId"
                )
            }

        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val json = objectMapper.readTree(packet.toJson())
        val vedtaksperiodeId = UUID.fromString(json["vedtaksperiodeId"].asText())
        val aktørId = json["aktørId"].asText()
        val fødselsnummer = json["fødselsnummer"].asText()
        val yrkesaktivitet = json["yrkesaktivitet"].asText()
        val fom = json["fom"].asLocalDate()
        val tom = json["tom"].asLocalDate()
        val skjæringstidspunkt = json["skjæringstidspunkt"].asLocalDate()
        val tilstand = json["tilstand"].asText()
        val oppdatert = json["oppdatert"].asLocalDateTime()
        try {
            sessionOf(dataSource).use { session ->
                val upsert = """
                    insert into vedtaksperiode_data (vedtaksperiodeId, fnr, aktorId, yrkesaktivitet, fom, tom, skjaeringstidspunkt, tilstand, oppdatert)
                    values (:vedtaksperiodeId, :fnr, :aktorId, :yrkesaktivitet, :fom, :tom, :skjaeringstidspunkt, :tilstand, :oppdatert)
                    on conflict(vedtaksperiodeId) do update
                        set fnr = excluded.fnr,  
                            aktorId = excluded.aktorId,
                            fom = excluded.fom,
                            tom = excluded.tom,
                            yrkesaktivitet = excluded.yrkesaktivitet,
                            skjaeringstidspunkt = excluded.skjaeringstidspunkt,
                            tilstand = excluded.tilstand,
                            oppdatert = excluded.oppdatert;
                """.trimIndent()
                session.update(
                    queryOf(
                        upsert, mapOf(
                            "vedtaksperiodeId" to vedtaksperiodeId,
                            "fnr" to fødselsnummer,
                            "aktorId" to aktørId,
                            "yrkesaktivitet" to yrkesaktivitet,
                            "fom" to fom,
                            "tom" to tom,
                            "skjaeringstidspunkt" to skjæringstidspunkt,
                            "tilstand" to tilstand,
                            "oppdatert" to oppdatert
                        )
                    )
                )
            }
        } catch (err: PSQLException) {
            logg.warn("klarte ikke oppdatere vedtaksdata fra dump")
            sikkerlogg.warn("klarte ikke oppdatere vedtaksdata fra dump: $packet")
        }
    }
}
