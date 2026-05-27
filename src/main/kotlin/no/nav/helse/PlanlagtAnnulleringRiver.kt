package no.nav.helse

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers.isMissingOrNull
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

class PlanlagtAnnulleringRiver(
    rapidApplication: RapidsConnection,
    private val dataSource: DataSource,
) : River.PacketListener {
    init {
        River(rapidApplication)
            .apply {
                precondition { it.requireValue("@event_name", "planlagt_annullering") }
                validate {
                    it.requireArray("vedtaksperioder")
                    it.requireKey("yrkesaktivitetstype", "@id")
                    it.interestedIn("organisasjonsnummer")
                }
            }.register(this)
    }

    override fun onError(
        problems: MessageProblems,
        context: MessageContext,
        metadata: MessageMetadata,
    ) {
        sikkerlogg.error("Klarte ikke å lese planlagt_annullering event! ${problems.toExtendedReport()}")
    }

    override fun onPacket(
        packet: JsonMessage,
        context: MessageContext,
        metadata: MessageMetadata,
        meterRegistry: MeterRegistry,
    ) {
        val hendelseId = UUID.fromString(packet["@id"].asText())
        val vedtaksperioder = packet["vedtaksperioder"].map { UUID.fromString(it.asText()) }
        val yrkesaktivitetstype = packet["yrkesaktivitetstype"].asText()
        val organisasjonsnummer = packet["organisasjonsnummer"].takeUnless { it.isMissingOrNull() }?.asText() ?: yrkesaktivitetstype

        val lagredeBerørteVedtaksperioder = sessionOf(dataSource).use { session ->
            session.transaction { tx ->
                val utløsendeVedtaksperiodeId = vedtaksperioder.firstOrNull { vedtaksperiodeId ->
                    tx.run(
                        queryOf(
                            "SELECT 1 FROM annullering WHERE id = ?",
                            vedtaksperiodeId.toString(),
                        ).map { it.int(1) }.asSingle,
                    ) != null
                }
                if (utløsendeVedtaksperiodeId == null) {
                    sikkerlogg.error("Ignorerer planlagt_annullering for hendelse $hendelseId fordi utløsende vedtaksperiode ikke finnes i annullering. Packet dump: ${packet.toJson()}")
                    return@transaction 0
                }

                @Language("PostgreSQL")
                val statement = """
                    INSERT INTO annullering_berorte_vedtaksperioder(
                        vedtaksperiode_id, utløsende_vedtaksperiode_id,
                        organisasjonsnummer, yrkesaktivitetstype
                    ) VALUES (?, ?, ?, ?)
                    ON CONFLICT DO NOTHING
                """
                vedtaksperioder.sumOf { vedtaksperiodeId ->
                        tx.run(
                            queryOf(
                                statement,
                                vedtaksperiodeId,
                                utløsendeVedtaksperiodeId,
                                organisasjonsnummer,
                                yrkesaktivitetstype,
                            ).asUpdate,
                        )
                    }
            }
        }
        if (lagredeBerørteVedtaksperioder > 0) {
            logg.info("Lagret $lagredeBerørteVedtaksperioder berørte vedtaksperioder fra planlagt_annullering for hendelse $hendelseId")
        }
    }
}
