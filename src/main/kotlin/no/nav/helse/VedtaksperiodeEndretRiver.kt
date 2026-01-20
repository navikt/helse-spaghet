package no.nav.helse

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDate
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import com.github.navikt.tbd_libs.result_object.getOrThrow
import com.github.navikt.tbd_libs.retry.retryBlocking
import com.github.navikt.tbd_libs.speed.SpeedClient
import io.micrometer.core.instrument.MeterRegistry
import kotliquery.queryOf
import kotliquery.sessionOf
import org.postgresql.util.PSQLException
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*
import javax.sql.DataSource

class VedtaksperiodeEndretRiver(
    rapidApplication: RapidsConnection,
    private val dataSource: DataSource,
    private val speedClient: SpeedClient,
) : River.PacketListener {
    init {
        River(rapidApplication)
            .apply {
                precondition { it.requireValue("@event_name", "vedtaksperiode_endret") }
                validate {
                    it.requireKey(
                        "@id",
                        "fødselsnummer",
                        "yrkesaktivitetstype",
                        "vedtaksperiodeId",
                        "gjeldendeTilstand",
                        "fom",
                        "tom",
                        "skjæringstidspunkt",
                    )
                    it.interestedIn("organisasjonsnummer")
                }
            }.register(this)
    }

    override fun onPacket(
        packet: JsonMessage,
        context: MessageContext,
        metadata: MessageMetadata,
        meterRegistry: MeterRegistry,
    ) {
        lagreVedtaksperiodedata(speedClient, packet["gjeldendeTilstand"].asText(), packet, dataSource)
    }
}

class VedtaksperiodeOpprettetRiver(
    rapidApplication: RapidsConnection,
    private val dataSource: DataSource,
    private val speedClient: SpeedClient,
) : River.PacketListener {
    init {
        River(rapidApplication)
            .apply {
                precondition { it.requireValue("@event_name", "vedtaksperiode_opprettet") }
                validate {
                    it.requireKey(
                        "@id",
                        "fødselsnummer",
                        "yrkesaktivitetstype",
                        "vedtaksperiodeId",
                        "skjæringstidspunkt",
                        "fom",
                        "tom",
                    )
                    it.interestedIn("organisasjonsnummer")
                }
            }.register(this)
    }

    override fun onPacket(
        packet: JsonMessage,
        context: MessageContext,
        metadata: MessageMetadata,
        meterRegistry: MeterRegistry,
    ) {
        lagreVedtaksperiodedata(speedClient, "START", packet, dataSource)
    }
}

private val minsteDato = LocalDate.of(-4500, 1, 1)

private fun lagreVedtaksperiodedata(
    speedClient: SpeedClient,
    tilstand: String,
    packet: JsonMessage,
    dataSource: DataSource,
) {
    val ident = packet["fødselsnummer"].asText()
    val callId = packet["@id"].asText()

    val identer = retryBlocking { speedClient.hentFødselsnummerOgAktørId(ident, callId).getOrThrow() }
    val vedtaksperiodeId = UUID.fromString(packet["vedtaksperiodeId"].asText())

    val yrkesaktivitet = when (val yrkesaktivitetstype = packet["yrkesaktivitetstype"].asText()) {
        "ARBEIDSTAKER" -> packet["organisasjonsnummer"].asText()
        else -> yrkesaktivitetstype
    }

    val fom = packet["fom"].asLocalDate()
    val tom = packet["tom"].asLocalDate()
    val skjæringstidspunkt = packet["skjæringstidspunkt"].asLocalDate().coerceAtLeast(minsteDato)
    try {
        sessionOf(dataSource).use { session ->
            val upsert =
                """
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
                    upsert,
                    mapOf(
                        "vedtaksperiodeId" to vedtaksperiodeId,
                        "fnr" to identer.fødselsnummer,
                        "aktorId" to identer.aktørId,
                        "yrkesaktivitet" to yrkesaktivitet,
                        "fom" to fom,
                        "tom" to tom,
                        "skjaeringstidspunkt" to skjæringstidspunkt,
                        "tilstand" to tilstand,
                        "oppdatert" to LocalDateTime.now(),
                    ),
                ),
            )
        }
    } catch (err: PSQLException) {
        logg.warn("klarte ikke oppdatere vedtaksdata fra dump")
        sikkerlogg.warn("klarte ikke oppdatere vedtaksdata fra dump; Error: $err \nPacket dump: ${packet.toJson()}")
    }
}
