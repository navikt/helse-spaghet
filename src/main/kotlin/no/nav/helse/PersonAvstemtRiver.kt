package no.nav.helse

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDate
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.Util.asUuid
import org.postgresql.util.PSQLException
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*
import javax.sql.DataSource

class VedtaksperiodeAvstemt(
    rapidApplication: RapidsConnection,
    private val dataSource: DataSource
) : River.PacketListener {
    init {
        River(rapidApplication).apply {
            validate {
                it.demandValue("@event_name", "person_avstemt")
                it.requireKey(
                    "aktørId",
                    "fødselsnummer"
                )
                it.requireArray("arbeidsgivere") {
                    requireKey("organisasjonsnummer")
                    requireArray("vedtaksperioder") {
                        requireKey("id", "tilstand", "oppdatert", "fom", "tom", "skjæringstidspunkt")
                    }
                }
            }

        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val fnr = packet["fødselsnummer"].asText()
        val aktørId = packet["aktørId"].asText()
        val data: List<VedtaksperiodeData> = packet["arbeidsgivere"].flatMap { arbeidsgiver ->
            val orgnr = arbeidsgiver["organisasjonsnummer"].asText()
            arbeidsgiver["vedtaksperioder"].map { vedtaksperiode ->
                VedtaksperiodeData(
                    id = vedtaksperiode["id"].asUuid(),
                    fnr = fnr,
                    aktørId = aktørId,
                    yrkesaktivitet = orgnr,
                    tilstand = vedtaksperiode["tilstand"].asText(),
                    fom = vedtaksperiode["fom"].asLocalDate(),
                    tom = vedtaksperiode["tom"].asLocalDate(),
                    skjæringstidspunkt = vedtaksperiode["skjæringstidspunkt"].asLocalDate(),
                    oppdatert = vedtaksperiode["oppdatert"].asLocalDateTime()
                )
            }
        }
        data.forEach { lagreVedtaksperiodedata(it, dataSource) }
    }
}

private data class VedtaksperiodeData(
    val id: UUID,
    val fnr: String,
    val aktørId: String,
    val yrkesaktivitet: String,
    val tilstand: String,
    val fom: LocalDate,
    val tom: LocalDate,
    val skjæringstidspunkt: LocalDate,
    val oppdatert: LocalDateTime
)

private fun lagreVedtaksperiodedata(data: VedtaksperiodeData, dataSource: DataSource) {
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
                        "vedtaksperiodeId" to data.id,
                        "fnr" to data.fnr,
                        "aktorId" to data.aktørId,
                        "yrkesaktivitet" to data.yrkesaktivitet,
                        "fom" to data.fom,
                        "tom" to data.tom,
                        "skjaeringstidspunkt" to data.skjæringstidspunkt,
                        "tilstand" to data.tilstand,
                        "oppdatert" to data.oppdatert
                    )
                )
            )
        }
    } catch (err: PSQLException) {
        logg.warn("klarte ikke oppdatere vedtaksdata fra person_avstemt", err)
    }
}