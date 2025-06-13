package no.nav.helse

import com.fasterxml.jackson.databind.node.ObjectNode
import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageProblems
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import com.github.navikt.tbd_libs.result_object.getOrThrow
import com.github.navikt.tbd_libs.retry.retryBlocking
import com.github.navikt.tbd_libs.speed.SpeedClient
import io.micrometer.core.instrument.MeterRegistry
import java.time.LocalDateTime
import java.util.UUID
import javax.sql.DataSource
import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.Util.jsonNode
import org.intellij.lang.annotations.Language

class AnalytiskDatapakkeRiver(
    rapidApplication: RapidsConnection,
    private val dataSource: DataSource,
    private val speedClient: SpeedClient
) : River.PacketListener {
    init {
        River(rapidApplication).apply {
            precondition { it.requireValue("@event_name", "analytisk_datapakke") }
            validate {
                it.requireKey(
                    "@opprettet",
                    "@id",
                    "behandlingId",
                    "vedtaksperiodeId",
                    "yrkesaktivitetstype",
                    "skjæringstidspunkt",
                    "fom",
                    "tom",
                    "harAndreInntekterIBeregning",
                    "antallGjenståendeSykedagerEtterPeriode.antallDager",
                    "antallGjenståendeSykedagerEtterPeriode.nettoDager",
                    "antallForbrukteSykedagerEtterPeriode.antallDager",
                    "antallForbrukteSykedagerEtterPeriode.nettoDager",
                    "beløpTilBruker.totalBeløp",
                    "beløpTilBruker.nettoBeløp",
                    "beløpTilArbeidsgiver.totalBeløp",
                    "beløpTilArbeidsgiver.nettoBeløp",
                    "fødselsnummer"
                )
            }
        }.register(this)
    }

    override fun onPacket(
        packet: JsonMessage,
        context: MessageContext,
        metadata: MessageMetadata,
        meterRegistry: MeterRegistry
    ) {

        val vedtaksperiodeId = UUID.fromString(packet["vedtaksperiodeId"].asText())
        val behandlingId = UUID.fromString(packet["behandlingId"].asText())
        val opprettet = packet["@opprettet"].asLocalDateTime()

        // Hent AktørId fra Speed
        val ident = packet["fødselsnummer"].asText()
        val callId = packet["@id"].asText()
        val aktorId = retryBlocking { speedClient.hentFødselsnummerOgAktørId(ident, callId).getOrThrow() }.aktørId

        // Kopier packet, ta vekk doble eller uinteressante felter
        val datapakke = packet.jsonNode().apply {
            this as ObjectNode
            remove(
                listOf(
                    "@event_name",
                    "@id",
                    "fødselsnummer",
                    "@opprettet",
                    "system_read_count",
                    "system_participating_services"
                )
            )
        }.toString()

        insertAnalytiskDatapakke(
            aktorId,
            opprettet,
            vedtaksperiodeId,
            behandlingId,
            datapakke
        )
        logg.info("Lagret Analytisk Datapakke for vedtaksperiodeId=$vedtaksperiodeId")
    }

    override fun onError(problems: MessageProblems, context: MessageContext, metadata: MessageMetadata) {
        sikkerlogg.error("Klarte ikke å lese analytisk_datapakke event! ${problems.toExtendedReport()}")
    }

    private fun insertAnalytiskDatapakke(
        aktorId: String,
        opprettet: LocalDateTime,
        vedtaksperiodeId: UUID,
        behandlingId: UUID,
        datapakke: String
    ) {
        @Language("PostgreSQL")
        val insertAnalytiskDatapakke =
            "INSERT INTO vedtaksdata(aktor_id, opprettet, vedtaksperiode_id, behandling_id, datapakke)" +
                "VALUES(:aktor_id, :opprettet, :vedtaksperiode_id, :behandling_id, :datapakke::jsonb)" +
                "ON CONFLICT DO NOTHING;"
        sessionOf(dataSource).use { session ->
            session.run(
                queryOf(
                    insertAnalytiskDatapakke, mapOf(
                        "aktor_id" to aktorId,
                        "opprettet" to opprettet,
                        "vedtaksperiode_id" to vedtaksperiodeId,
                        "behandling_id" to behandlingId,
                        "datapakke" to datapakke
                    )
                ).asUpdate
            )
        }
    }
}

