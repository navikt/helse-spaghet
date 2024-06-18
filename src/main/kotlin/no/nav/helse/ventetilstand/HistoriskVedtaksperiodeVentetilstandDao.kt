package no.nav.helse.ventetilstand

import kotliquery.Query
import kotliquery.queryOf
import kotliquery.sessionOf
import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.helse.ventetilstand.VedtaksperiodeVenter.Companion.vedtaksperiodeVenter
import org.intellij.lang.annotations.Language
import org.slf4j.LoggerFactory
import java.util.*
import javax.sql.DataSource

internal class HistoriskVedtaksperiodeVentetilstandDao(private val dataSource: DataSource): VedtaksperiodeVentetilstandDao {

    override fun venter(vedtaksperiodeVenter: VedtaksperiodeVenter, hendelse: Hendelse) {
        val vedtaksperiodeId = vedtaksperiodeVenter.vedtaksperiodeId
        if (hentOmVenter(vedtaksperiodeId) == vedtaksperiodeVenter) return logger.info("Ingen endring på ventetilstand for {}", keyValue("vedtaksperiodeId", vedtaksperiodeId))
        nyGjeldende(vedtaksperiodeVenter, hendelse, queryOf(VENTER, mapOf(
            "hendelseId" to hendelse.id,
            "hendelse" to hendelse.hendelse,
            "vedtaksperiodeId" to vedtaksperiodeVenter.vedtaksperiodeId,
            "skjaeringstidspunkt" to vedtaksperiodeVenter.skjæringstidspunkt,
            "fodselsnummer" to vedtaksperiodeVenter.fødselsnummer,
            "organisasjonsnummer" to vedtaksperiodeVenter.organisasjonsnummer,
            "ventetSiden" to vedtaksperiodeVenter.ventetSiden,
            "venterTil" to vedtaksperiodeVenter.venterTil,
            "venterPaVedtaksperiodeId" to vedtaksperiodeVenter.venterPå.vedtaksperiodeId,
            "venterPaSkjaeringstidspunkt" to vedtaksperiodeVenter.venterPå.skjæringstidspunkt,
            "venterPaOrganisasjonsnummer" to vedtaksperiodeVenter.venterPå.organisasjonsnummer,
            "venterPaHva" to vedtaksperiodeVenter.venterPå.hva,
            "venterPaHvorfor" to vedtaksperiodeVenter.venterPå.hvorfor
        )))
        logger.info("Lagret ny ventetilstand for {}", keyValue("vedtaksperiodeId", vedtaksperiodeId))
    }

    override fun venterIkke(vedtaksperiodeId: UUID, hendelse: Hendelse) {
        val vedtaksperiodeVentet = hentOmVenter(vedtaksperiodeId) ?: return
        nyGjeldende(vedtaksperiodeVentet, hendelse, queryOf(VENTER_IKKE, mapOf(
            "hendelseId" to hendelse.id,
            "hendelse" to hendelse.hendelse,
            "vedtaksperiodeId" to vedtaksperiodeVentet.vedtaksperiodeId,
            "skjaeringstidspunkt" to vedtaksperiodeVentet.skjæringstidspunkt,
            "fodselsnummer" to vedtaksperiodeVentet.fødselsnummer,
            "organisasjonsnummer" to vedtaksperiodeVentet.organisasjonsnummer
        )))
    }

    private fun hentOmVenter(vedtaksperiodeId: UUID): VedtaksperiodeVenter? {
        return sessionOf(dataSource).use { session ->
            session.single(
                queryOf(HENT_OM_VENTER, mapOf("vedtaksperiodeId" to vedtaksperiodeId))
            ){ row -> row.vedtaksperiodeVenter }
        }
    }

    private fun nyGjeldende(vedtaksperiodeVenter: VedtaksperiodeVenter, hendelse: Hendelse, nyGjeldendeQuery: Query) {
        sessionOf(dataSource).use { session ->
            session.transaction {
                it.execute(queryOf(IKKE_LENGER_GJELDENDE, mapOf("vedtaksperiodeId" to vedtaksperiodeVenter.vedtaksperiodeId, "hendelseId" to hendelse.id)))
                it.execute(nyGjeldendeQuery)
            }
        }
    }

    internal companion object {
        private val logger = LoggerFactory.getLogger(HistoriskVedtaksperiodeVentetilstandDao::class.java)

        @Language("PostgreSQL")
        private val HENT_OM_VENTER = "SELECT * FROM vedtaksperiode_ventetilstand WHERE vedtaksperiodeId = :vedtaksperiodeId AND gjeldende = true AND venter = true"

        @Language("PostgreSQL")
        private val VENTER = """
            INSERT INTO vedtaksperiode_ventetilstand(hendelseId, hendelse, venter, vedtaksperiodeId, skjaeringstidspunkt, fodselsnummer, organisasjonsnummer, ventetSiden, venterTil, venterPaVedtaksperiodeId, venterPaSkjaeringstidspunkt, venterPaOrganisasjonsnummer, venterPaHva, venterPaHvorfor, gjeldende)
            VALUES (:hendelseId, :hendelse::jsonb, true, :vedtaksperiodeId, :skjaeringstidspunkt, :fodselsnummer, :organisasjonsnummer, :ventetSiden, :venterTil, :venterPaVedtaksperiodeId, :venterPaSkjaeringstidspunkt, :venterPaOrganisasjonsnummer, :venterPaHva, :venterPaHvorfor, true) 
            ON CONFLICT (hendelseId) DO NOTHING
        """

        @Language("PostgreSQL")
        private val VENTER_IKKE = """
            INSERT INTO vedtaksperiode_ventetilstand(hendelseId, hendelse, venter, vedtaksperiodeId, skjaeringstidspunkt, fodselsnummer, organisasjonsnummer, gjeldende)
            VALUES (:hendelseId, :hendelse::jsonb, false, :vedtaksperiodeId, :skjaeringstidspunkt, :fodselsnummer, :organisasjonsnummer, true)
            ON CONFLICT (hendelseId) DO NOTHING 
        """

        @Language("PostgreSQL")
        private val IKKE_LENGER_GJELDENDE = """
            UPDATE vedtaksperiode_ventetilstand
            SET gjeldende = false
            WHERE vedtaksperiodeId = :vedtaksperiodeId
            AND gjeldende = true
            AND hendelseId != :hendelseId
        """
    }
}