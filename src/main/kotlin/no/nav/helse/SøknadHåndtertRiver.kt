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
import org.intellij.lang.annotations.Language
import java.time.LocalDateTime
import java.util.*
import javax.sql.DataSource

class SøknadHåndtertRiver(
    rapidApplication: RapidsConnection,
    private val dataSource: DataSource,
) : River.PacketListener {
    init {
        River(rapidApplication)
            .apply {
                precondition { it.requireValue("@event_name", "søknad_håndtert") }
                validate {
                    it.requireKey("søknadId", "vedtaksperiodeId", "@opprettet")
                }
            }.register(this)
    }

    override fun onPacket(
        packet: JsonMessage,
        context: MessageContext,
        metadata: MessageMetadata,
        meterRegistry: MeterRegistry,
    ) {
        val søknadHendelseId = UUID.fromString(packet["søknadId"].asText())
        val vedtaksperiodeId = UUID.fromString(packet["vedtaksperiodeId"].asText())
        val opprettet = packet["@opprettet"].asLocalDateTime()

        insertSøknadHåndtert(søknadHendelseId, vedtaksperiodeId, opprettet)
        logg.info("Lagrer kobling mellom søknad $søknadHendelseId og vedtaksperiode $vedtaksperiodeId")
    }

    private fun insertSøknadHåndtert(
        søknadHendelseId: UUID,
        vedtaksperiodeId: UUID,
        opprettet: LocalDateTime,
    ) {
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = """
                INSERT INTO soknad_haandtert(soknad_hendelse_id, vedtaksperiode_id, opprettet) 
                VALUES(:soknad_hendelse_id, :vedtaksperiode_id, :opprettet)
                ON CONFLICT DO NOTHING
            """
            session.run(
                queryOf(
                    query,
                    mapOf(
                        "soknad_hendelse_id" to søknadHendelseId,
                        "vedtaksperiode_id" to vedtaksperiodeId,
                        "opprettet" to opprettet,
                    ),
                ).asExecute,
            )
        }
    }
}
