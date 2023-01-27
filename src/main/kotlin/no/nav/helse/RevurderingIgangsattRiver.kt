package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.*
import org.intellij.lang.annotations.Language
import java.util.*
import javax.sql.DataSource

class RevurderingIgangsattRiver(
    rapidApplication: RapidsConnection,
    private val dataSource: DataSource
) : River.PacketListener {
    init {
        River(rapidApplication).apply {
            validate {
                it.demandValue("@event_name", "overstyring_igangsatt")
                it.demandValue("typeEndring", "REVURDERING")
                it.require("revurderingId") { id -> UUID.fromString(id.asText()) }
                it.require("@opprettet", JsonNode::asLocalDateTime)
                it.require("skjæringstidspunkt", JsonNode::asLocalDate)
                it.require("periodeForEndringFom", JsonNode::asLocalDate)
                it.require("periodeForEndringTom", JsonNode::asLocalDate)
                it.require("kilde") { kilde -> UUID.fromString(kilde.asText()) }
                it.requireKey("årsak")
                it.requireArray("berørtePerioder") {
                    require("vedtaksperiodeId") { vedtaksperiodeId -> UUID.fromString(vedtaksperiodeId.asText()) }
                    require("periodeFom", JsonNode::asLocalDate)
                    require("periodeTom", JsonNode::asLocalDate)
                    require("skjæringstidspunkt", JsonNode::asLocalDate)
                    requireKey("orgnummer")
                }
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val revurderingId = packet["revurderingId"].let { UUID.fromString(it.asText()) }
        val årsak = packet["årsak"].asText()
        val opprettet = packet["@opprettet"].asLocalDateTime()
        val skjæringstidspunkt = packet["skjæringstidspunkt"].asLocalDate()
        val kilde = packet["kilde"].let { UUID.fromString(it.asText()) }
        val periodeForEndringFom = packet["periodeForEndringFom"].asLocalDate()
        val periodeForEndringTom = packet["periodeForEndringTom"].asLocalDate()
        val berørtePerioder = packet["berørtePerioder"]

        log.info("Legger inn data fra revurdering_igangsatt i databasen")

        sessionOf(dataSource).use {
            it.transaction { session ->
                @Language("PostgreSQL")
                val statement = """
                     INSERT INTO revurdering(id, opprettet, kilde, skjaeringstidspunkt, periode_for_endring_fom, periode_for_endring_tom, aarsak)
                     VALUES (:id, :opprettet, :kilde, :skjaeringstidspunkt, :fom, :tom, :aarsak)
                     ON CONFLICT DO NOTHING
                """
                session.run(
                    queryOf(
                        statement = statement,
                        paramMap = mapOf(
                            "id" to revurderingId,
                            "opprettet" to opprettet,
                            "kilde" to kilde,
                            "skjaeringstidspunkt" to skjæringstidspunkt,
                            "fom" to periodeForEndringFom,
                            "tom" to periodeForEndringTom,
                            "aarsak" to årsak,
                        )
                    ).asExecute
                )

                @Language("PostgreSQL")
                val statement2 = """
                    INSERT INTO revurdering_vedtaksperiode(revurdering_id, vedtaksperiode_id, periode_fom, periode_tom, skjaeringstidspunkt, orgnummer)
                    VALUES ${berørtePerioder.joinToString { "(?, ?, ?, ?, ?, ?)" }}
                    ON CONFLICT DO NOTHING
                """

                session.run(
                    queryOf(
                        statement = statement2,
                        *berørtePerioder.flatMap { periode ->
                            listOf(
                                revurderingId,
                                periode.path("vedtaksperiodeId").let { UUID.fromString(it.asText()) },
                                periode.path("periodeFom").asLocalDate(),
                                periode.path("periodeTom").asLocalDate(),
                                periode.path("skjæringstidspunkt").asLocalDate(),
                                periode.path("orgnummer").asText(),
                            )
                        }.toTypedArray()
                    ).asExecute
                )
            }
        }
    }
}
