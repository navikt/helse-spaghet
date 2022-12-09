package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.*
import org.intellij.lang.annotations.Language
import java.util.UUID
import javax.sql.DataSource

class RevurderingIgangsattRiver(
    rapidApplication: RapidsConnection,
    private val dataSource: DataSource
) : River.PacketListener {
    init {
        River(rapidApplication).apply {
            validate {
                it.demandValue("@event_name", "revurdering_igangsatt")
                it.require("@opprettet", JsonNode::asLocalDateTime)
                it.require("skjæringstidspunkt", JsonNode::asLocalDate)
                it.require("periodeForEndringFom", JsonNode::asLocalDate)
                it.require("periodeForEndringTom", JsonNode::asLocalDate)
                it.require("kilde") { kilde -> UUID.fromString(kilde.asText()) }
                it.requireKey("fødselsnummer", "aktørId", "årsak", "typeEndring")
                it.requireArray("berørtePerioder") {
                    require("vedtaksperiodeId") { vedtaksperiodeId -> UUID.fromString(vedtaksperiodeId.asText()) }
                    require("periodeFom", JsonNode::asLocalDate)
                    require("periodeTom", JsonNode::asLocalDate)
                    require("skjæringstidspunkt", JsonNode::asLocalDate)
                    requireKey("orgnummer", "typeEndring")
                }
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val årsak = packet["årsak"].asText()
        val opprettet = packet["@opprettet"].asLocalDateTime()
        val skjæringstidspunkt = packet["skjæringstidspunkt"].asLocalDate()
        val fødselsnummer = packet["fødselsnummer"].asText()
        val aktørId = packet["aktørId"].asText()
        val kilde = packet["kilde"].let { UUID.fromString(it.asText()) }
        val periodeForEndringFom = packet["periodeForEndringFom"].asLocalDate()
        val periodeForEndringTom = packet["periodeForEndringTom"].asLocalDate()
        val typeEndring = packet["typeEndring"].asText()
        val berørtePerioder = packet["berørtePerioder"]

        log.info("Legger inn data fra revurdering_igangsatt i databasen")

        sessionOf(dataSource, returnGeneratedKey = true).use {
            it.transaction { session ->
                @Language("PostgreSQL")
                val statement = """
                     INSERT INTO revurdering_igangsatt(opprettet, type_endring, fodselsnummer, aktor_id, kilde, skjaeringstidspunkt, periode_for_endring_fom, periode_for_endring_tom, aarsak)
                     VALUES (:opprettet, :type_endring, :fnr, :aktor_id, :kilde, :skjaeringstidspunkt, :fom, :tom, :aarsak)
                      """
                val revurderingId = session.run(
                    queryOf(
                        statement = statement,
                        paramMap = mapOf(
                            "opprettet" to opprettet,
                            "type_endring" to typeEndring,
                            "fnr" to fødselsnummer,
                            "aktor_id" to aktørId,
                            "kilde" to kilde,
                            "skjaeringstidspunkt" to skjæringstidspunkt,
                            "fom" to periodeForEndringFom,
                            "tom" to periodeForEndringTom,
                            "aarsak" to årsak,
                        )
                    ).asUpdateAndReturnGeneratedKey
                ) ?: return@transaction

                @Language("PostgreSQL")
                val statement2 = """
                    INSERT INTO revurdering_igangsatt_vedtaksperiode(revurdering_igangsatt_id, vedtaksperiode_id, periode_fom, periode_tom, skjaeringstidspunkt, orgnummer, type_endring)
                    VALUES ${berørtePerioder.joinToString { "(?, ?, ?, ?, ?, ?, ?)" }}
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
                                periode.path("typeEndring").asText()
                            )
                        }.toTypedArray()
                    ).asExecute
                )
            }
        }
    }
}
