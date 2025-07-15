package no.nav.helse

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers.toUUID
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import com.github.navikt.tbd_libs.result_object.getOrThrow
import com.github.navikt.tbd_libs.result_object.map
import com.github.navikt.tbd_libs.result_object.tryCatch
import com.github.navikt.tbd_libs.spedisjon.SpedisjonClient
import io.micrometer.core.instrument.MeterRegistry
import java.time.LocalDateTime
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.UUID
import javax.sql.DataSource
import kotliquery.queryOf
import kotliquery.sessionOf
import org.intellij.lang.annotations.Language

internal class InntektsmeldingHåndtertRiver(
    rapidsConnection: RapidsConnection,
    private val dataSource: DataSource,
    private val spedisjonClient: SpedisjonClient,
) : River.PacketListener {

    private companion object {
        private val logg: Logger = LoggerFactory.getLogger(InntektsmeldingHåndtertRiver::class.java)
    }

    init {
        River(rapidsConnection).apply {
            precondition { it.requireValue("@event_name", "inntektsmelding_håndtert") }
            validate {
                it.requireKey(
                    "vedtaksperiodeId",
                    "inntektsmeldingId",
                    "@opprettet"
                )
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        val vedtaksperiodeId = packet["vedtaksperiodeId"].asText().toUUID()
        val hendelseId = packet["inntektsmeldingId"].asText().toUUID()
        val opprettet = packet["@opprettet"].asLocalDateTime()

        // Hent ekstern dokument ID fra spedisjon
        val dokumentId = try {
            spedisjonClient.hentMelding(hendelseId).getOrThrow().eksternDokumentId
        } catch (exception: RuntimeException) {
            // I dev så kan det hende at spedisjon ikke har inntektsmedlingen, da Spleis-testdata sender den rett på rapid.
            // Så i dev så setter vi en random UUID som dokumentId hvis den ikke finnes. I prod er dette en feil.
            if (System.getenv("NAIS_CLUSTER_NAME") == "dev-gcp") {
                UUID.randomUUID()
            } else {
                sikkerlogg.error("Kunne ikke hente ekstern dokument ID for inntektsmelding $hendelseId", exception)
                logg.error("Feil ved henting av ekstern dokument ID for inntektsmelding $hendelseId", exception)
                throw exception
            }
        }
        insertInnektsmeldingHåndtert(vedtaksperiodeId, hendelseId, dokumentId, opprettet)
    }

    private fun insertInnektsmeldingHåndtert(
        vedtaksperiodeId: UUID,
        hendelseId: UUID,
        dokumentId: UUID,
        opprettet: LocalDateTime
    ) {
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query =
                """INSERT INTO inntektsmelding_haandtert(vedtaksperiode_id, inntektsmelding_id, dokument_id, opprettet) VALUES(:vedtaksperiodeId, :hendelseId, :dokumentId, :opprettet) ON CONFLICT DO NOTHING"""
            session.run(
                queryOf(
                    query,
                    mapOf(
                        "vedtaksperiodeId" to vedtaksperiodeId,
                        "hendelseId" to hendelseId,
                        "dokumentId" to dokumentId,
                        "opprettet" to opprettet
                    )
                ).asExecute
            )
        }
        logg.info("Lagrer inntektsmelding håndtert for vedtaksperiode $vedtaksperiodeId og inntektsmelding $hendelseId")
    }
}

