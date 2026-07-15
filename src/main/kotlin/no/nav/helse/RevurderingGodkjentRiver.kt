package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageProblems
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import kotliquery.queryOf
import kotliquery.sessionOf
import org.intellij.lang.annotations.Language
import java.util.*
import javax.sql.DataSource

class RevurderingGodkjentRiver(
    rapidApplication: RapidsConnection,
    private val dataSource: DataSource,
) : River.PacketListener {
    init {
        River(rapidApplication)
            .apply {
                precondition {
                    it.requireAll("@behov", listOf("Godkjenning"))
                    it.requireContains("Godkjenning.tags", "Revurdering")
                    it.forbid("@final")
                }
                validate {
                    it.require("@løsning.Godkjenning", ::tilLøsning)
                    it.requireKey("vedtaksperiodeId", "@opprettet")
                }
            }.register(this)
    }

    override fun onError(problems: MessageProblems, context: MessageContext, metadata: MessageMetadata) {
        logg.error(problems.toString())
        super.onError(problems, context, metadata)
    }

    override fun onPacket(
        packet: JsonMessage,
        context: MessageContext,
        metadata: MessageMetadata,
        meterRegistry: MeterRegistry,
    ) {
        val opprettet = packet["@opprettet"].asLocalDateTime()
        val løsning = tilLøsning(packet["@løsning.Godkjenning"])
        val status = when {
            løsning.godkjent && løsning.automatiskBehandling -> "FERDIGSTILT_AUTOMATISK"
            løsning.godkjent && !løsning.automatiskBehandling -> "FERDIGSTILT_MANUELT"
            !løsning.godkjent && løsning.automatiskBehandling -> "AVVIST_AUTOMATISK"
            else -> "AVVIST_MANUELT"
        }
        val vedtaksperiodeId = UUID.fromString(packet["vedtaksperiodeId"].asText())
        val revurdering = hentRevurdering(vedtaksperiodeId)

        val erRevurderingFerdig = revurdering.second.filterNot { it.first == vedtaksperiodeId }
            .all { it.second in listOf("FERDIGSTILT_AUTOMATISK", "FERDIGSTILT_MANUELT", "AVVIST_AUTOMATISK", "AVVIST_MANUELT") }

        logg.info("Legger inn data fra godkjenningsbehov i databasen")

        sessionOf(dataSource).use {
            it.transaction { session ->
                if (erRevurderingFerdig) {
                    session.run(
                        queryOf(
                            statement = statement,
                            paramMap =
                                mapOf(
                                    "revurderingId" to revurdering.first,
                                    "oppdatert" to opprettet,
                                    "status" to status,
                                ),
                        ).asUpdate,
                    )
                }

                session.run(
                    queryOf(
                        statement2,
                        mapOf(
                            "status" to status,
                            "vedtaksperiodeId" to vedtaksperiodeId,
                            "revurderingId" to revurdering.first,
                        ),
                    ).asUpdate,
                )
            }
        }
    }

    private fun tilLøsning(jsonNode: JsonNode) =
        Godkjenningsbehov.Løsning(
            godkjent = jsonNode["godkjent"].asBoolean(),
            saksbehandlerIdent = jsonNode["saksbehandlerIdent"].asText(),
            godkjentTidspunkt = jsonNode["godkjentTidspunkt"].asLocalDateTime(),
            automatiskBehandling = jsonNode["automatiskBehandling"].asBoolean(),
            årsak = null,
            begrunnelser = null,
            kommentar = null,
        )

    private fun hentRevurdering(vedtaksperiodeId: UUID): Pair<UUID, List<Pair<UUID, String>>> {
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = """SELECT revurdering_id FROM revurdering_vedtaksperiode where vedtaksperiode_id='$vedtaksperiodeId' LIMIT 1;"""
            val revurderingId = requireNotNull(
                session.run(queryOf(query).map { row -> row.uuid("revurdering_id") }.asSingle),
            )

            @Language("PostgreSQL")
            val query2 = """SELECT vedtaksperiode_id, status FROM revurdering_vedtaksperiode WHERE revurdering_id='$revurderingId';"""
            val vedtaksperioder = session.run(queryOf(query2).map { row -> (row.uuid("vedtaksperiode_id") to row.string("status")) }.asList)
            return revurderingId to vedtaksperioder
        }
    }

    private companion object {
        @Language("PostgreSQL")
        val statement = """ UPDATE revurdering SET status=CAST(:status as revurderingstatus), oppdatert=:oppdatert WHERE id=:revurderingId """

        @Language("PostgreSQL")
        val statement2 = """ UPDATE revurdering_vedtaksperiode SET status=CAST(:status as revurderingstatus) WHERE vedtaksperiode_id=:vedtaksperiodeId AND revurdering_id=:revurderingId """
    }
}
