package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.*
import org.postgresql.util.PSQLException
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*
import javax.sql.DataSource

class VedtaksperiodeEndretRiver(
    rapidApplication: RapidsConnection,
    private val dataSource: DataSource
) : River.PacketListener {
    init {
        River(rapidApplication).apply {
            validate {
                it.demandValue("@event_name", "vedtaksperiode_endret")
                it.requireKey(
                    "aktørId",
                    "fødselsnummer",
                    "organisasjonsnummer",
                    "vedtaksperiodeId",
                    "gjeldendeTilstand",
                    "fom",
                    "tom",
                    "skjæringstidspunkt"
                )
            }

        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        lagreVedtaksperiodedata(packet, dataSource)
    }
}
class VedtaksperiodeOpprettetRiver(
    rapidApplication: RapidsConnection,
    private val dataSource: DataSource
) : River.PacketListener {
    init {
        River(rapidApplication).apply {
            validate {
                it.demandValue("@event_name", "vedtaksperiode_opprettet")
                it.requireKey(
                    "aktørId",
                    "fødselsnummer",
                    "organisasjonsnummer",
                    "vedtaksperiodeId",
                    "skjæringstidspunkt",
                    "fom",
                    "tom"
                )
            }

        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        lagreVedtaksperiodedata(packet, dataSource)
    }
}

private val minsteDato = LocalDate.of(-4500, 1, 1)

private fun lagreVedtaksperiodedata(packet: JsonMessage, dataSource: DataSource) {
    val json = objectMapper.readTree(packet.toJson())
    val vedtaksperiodeId = UUID.fromString(json["vedtaksperiodeId"].asText())
    val aktørId = json["aktørId"].asText()
    val fødselsnummer = json["fødselsnummer"].asText()
    val yrkesaktivitet = json["organisasjonsnummer"].asText()
    val fom = json["fom"].asLocalDate()
    val tom = json["tom"].asLocalDate()
    val skjæringstidspunkt = json["skjæringstidspunkt"].asLocalDate().coerceAtLeast(minsteDato)
    val tilstand = json["gjeldendeTilstand"]?.asText() ?: "START"
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
                        "oppdatert" to LocalDateTime.now()
                    )
                )
            )
        }
    } catch (err: PSQLException) {
        logg.warn("klarte ikke oppdatere vedtaksdata fra dump")
        sikkerlogg.warn("klarte ikke oppdatere vedtaksdata fra dump; Error: $err \nPacket dump: $json")
    }
}