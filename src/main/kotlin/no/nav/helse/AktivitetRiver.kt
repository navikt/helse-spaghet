package no.nav.helse

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.sykepenger.libs.logging.loggError
import org.intellij.lang.annotations.Language
import tools.jackson.databind.JsonNode
import java.time.LocalDateTime
import java.util.*
import javax.sql.DataSource

class AktivitetRiver(
    rapidApplication: RapidsConnection,
    private val dataSource: DataSource,
) : River.PacketListener {
    init {
        River(rapidApplication)
            .apply {
                precondition { it.requireValue("@event_name", "aktivitetslogg_ny_aktivitet") }
                validate {
                    it.requireKey("@id", "@forårsaket_av.id", "aktiviteter")
                    it.requireArray("aktiviteter") {
                        requireKey("nivå", "melding")
                        require("tidsstempel", JsonNode::asLocalDateTime)
                        requireArray("kontekster") {
                            requireKey("konteksttype", "kontekstmap")
                        }
                    }
                }
            }.register(this)
    }

    override fun onPacket(
        packet: JsonMessage,
        context: MessageContext,
        metadata: MessageMetadata,
        meterRegistry: MeterRegistry,
    ) {
        try {
            // log.info("Inserter aktiviteter for vedtaksperiodeId: ${json["vedtaksperiodeId"].asString()}")
            packet["aktiviteter"]
                .filter { aktivitet ->
                    aktivitet["nivå"].asString() in Nivå.values().map(Enum<*>::name)
                }.forEach { aktivitet ->
                    val vedtaksperiodeId =
                        aktivitet.path("kontekster").firstNotNullOfOrNull { kontekst ->
                            if (kontekst.path("konteksttype").asString() == "Vedtaksperiode") {
                                kontekst
                                    .path("kontekstmap")
                                    .path("vedtaksperiodeId")
                                    .takeIf { it.isString }
                                    ?.asString()
                                    ?.let { UUID.fromString(it) }
                            } else {
                                null
                            }
                        } ?: return@forEach
                    insertAktivitet(
                        id = UUID.fromString(packet["@id"].asString()),
                        vedtaksperiodeId = vedtaksperiodeId,
                        melding = aktivitet["melding"].asString(),
                        level = Nivå.valueOf(aktivitet["nivå"].asString()).gammeltNavn,
                        tidsstempel = aktivitet["tidsstempel"].asLocalDateTime(),
                        kilde = UUID.fromString(packet["@forårsaket_av.id"].asString()),
                    )
                }
        } catch (e: Exception) {
            loggError("Feilet ved inserting av aktiviteter", e, "id" to packet["@id"].asString())
        }
    }

    private fun insertAktivitet(
        id: UUID,
        vedtaksperiodeId: UUID,
        melding: String,
        level: String,
        tidsstempel: LocalDateTime,
        kilde: UUID,
    ) {
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query =
                """INSERT INTO vedtaksperiode_aktivitet(id, vedtaksperiode_id, melding, level, tidsstempel, dato, kilde) VALUES(:id, :vedtaksperiode_id, :melding, :level, :tidsstempel, :dato, :kilde) ON CONFLICT DO NOTHING"""
            session.run(
                queryOf(
                    query,
                    mapOf(
                        "id" to id,
                        "vedtaksperiode_id" to vedtaksperiodeId,
                        "melding" to melding,
                        "level" to level,
                        "tidsstempel" to tidsstempel,
                        "dato" to tidsstempel.toLocalDate(),
                        "kilde" to kilde,
                    ),
                ).asUpdate,
            )
        }
    }

    enum class Nivå(
        val gammeltNavn: String,
    ) {
        INFO("INFO"),
        BEHOV("BEHOV"),
        VARSEL("WARN"),
        FUNKSJONELL_FEIL("ERROR"),
        LOGISK_FEIL("SEVERE"),
    }
}
