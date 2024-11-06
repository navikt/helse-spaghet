package no.nav.helse

import kotliquery.TransactionalSession
import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.Util.asUuid
import no.nav.helse.rapids_rivers.*
import org.intellij.lang.annotations.Language
import java.time.LocalDateTime
import java.util.*
import javax.sql.DataSource

class OppgaveEndretRiver(
    rapidsConnection: RapidsConnection,
    private val dataSource: DataSource
): River.PacketListener {
    init {
        River(rapidsConnection).apply {
            validate {
                it.demandAny("@event_name", listOf("oppgave_opprettet", "oppgave_oppdatert"))
                it.requireKey("oppgaveId", "fødselsnummer", "@opprettet", "behandlingId")
                it.requireKey("tilstand")
                it.requireArray("egenskaper")
                it.interestedIn("saksbehandler")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val oppgaveEndret = OppgaveEndret(
            id = packet["oppgaveId"].asLong(),
            fødselsnummer = packet["fødselsnummer"].asText(),
            behandlingId = packet["behandlingId"].asUuid(),
            tilstand = packet["tilstand"].asText(),
            egenskaper = packet["egenskaper"].map { it.asText() },
            tildelt = !packet["saksbehandler"].isMissingOrNull(),
            opprettet = packet["@opprettet"].asLocalDateTime()
        )
        oppgaveEndret.lagre()
    }

    private data class OppgaveEndret(
        val id: Long,
        val fødselsnummer: String,
        val behandlingId: UUID,
        val tilstand: String,
        val egenskaper: List<String>,
        val tildelt: Boolean,
        val opprettet: LocalDateTime
    )

    private fun OppgaveEndret.lagre() {
        sessionOf(dataSource).use { session ->
            session.transaction {
                it.lagreOppgave(this)
                it.lagreOppgaveendring(this)
            }
        }
    }

    private fun TransactionalSession.lagreOppgave(oppgaveEndret: OppgaveEndret) {
        @Language("PostgreSQL")
        val query = "INSERT INTO oppgave (id, fødselsnummer, behandling_id, opprettet) VALUES(:id, :fodselsnummer, :behandlingId, :opprettet) ON CONFLICT DO NOTHING"
        run(
            queryOf(
                query, mapOf(
                    "id" to oppgaveEndret.id,
                    "fodselsnummer" to oppgaveEndret.fødselsnummer,
                    "behandlingId" to oppgaveEndret.behandlingId,
                    "opprettet" to oppgaveEndret.opprettet
                )
            ).asUpdate
        )
    }

    private fun TransactionalSession.lagreOppgaveendring(oppgaveEndret: OppgaveEndret) {
        val egenskaperForDatabase = oppgaveEndret.egenskaper.joinToString { """ "$it" """ }
        @Language("PostgreSQL")
        val query =
            """
                INSERT INTO oppgave_endret (oppgave_ref, egenskaper, tilstand, endret_tidspunkt, tildelt) 
                VALUES(:oppgave_ref, '{$egenskaperForDatabase}', :tilstand, :tidspunkt, :tildelt)
            """
        run(
            queryOf(
                query, mapOf(
                    "oppgave_ref" to oppgaveEndret.id,
                    "tilstand" to oppgaveEndret.tilstand,
                    "tidspunkt" to oppgaveEndret.opprettet,
                    "tildelt" to oppgaveEndret.tildelt,
                )
            ).asUpdate
        )
    }
}
