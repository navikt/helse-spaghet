package no.nav.helse

import kotliquery.*
import java.time.LocalDateTime
import java.util.*
import javax.sql.DataSource

fun DataSource.insertGodkjenning(behov: Godkjenningsbehov) =
    using(sessionOf(this, returnGeneratedKey = true)) { session ->
        session.transaction { transaction ->
            val godkjenningId = transaction.run(
                queryOf(
                    """
                        INSERT INTO godkjenning(
                            vedtaksperiode_id,
                            aktor_id,
                            fodselsnummer,
                            godkjent_av,
                            godkjent_tidspunkt,
                            godkjent,
                            arsak,
                            kommentar,
                            periodetype,
                            inntektskilde,
                            utbetaling_type,
                            refusjon_type,
                            er_saksbehandleroverstyringer
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    behov.vedtaksperiodeId,
                    behov.aktørId,
                    behov.fødselsnummer,
                    behov.løsning.saksbehandlerIdent,
                    behov.løsning.godkjentTidspunkt,
                    behov.løsning.godkjent,
                    behov.løsning.årsak,
                    behov.løsning.kommentar,
                    behov.periodetype,
                    behov.inntektskilde,
                    behov.utbetalingType,
                    behov.refusjonType,
                    behov.saksbehandleroverstyringer.isNotEmpty()
                ).asUpdateAndReturnGeneratedKey
            )
            behov.løsning.begrunnelser?.forEach { begrunnelse ->
                transaction.run(
                    queryOf("INSERT INTO begrunnelse(godkjenning_ref, tekst) VALUES(?, ?);", godkjenningId, begrunnelse)
                        .asUpdate
                )
            }
            behov.saksbehandleroverstyringer.forEach {
                transaction.run(
                    queryOf("""
                        INSERT INTO godkjenning_overstyringer(godkjenning_ref, overstyring_hendelse_id) 
                        VALUES(:godkjenningRef, :overstyringHendelseId);""",
                        mapOf(
                            "godkjenningRef" to godkjenningId,
                            "overstyringHendelseId" to it
                        )
                    ).asUpdate
                )
            }
        }
    }

fun Session.findGodkjenningId(vedtaksperiodeId: UUID): Int? =
    this.run(
        queryOf(
            """
SELECT id
FROM godkjenning g
WHERE g.vedtaksperiode_id = ?
ORDER BY g.godkjent_tidspunkt DESC
LIMIT 1
    """, vedtaksperiodeId
        )
            .map { it.int("id") }.asSingle
    )

fun DataSource.godkjenningAlleredeLagret(vedtaksperiodeId: UUID, godkjentTidspunkt: LocalDateTime): Boolean =
    using(sessionOf(this)) { session ->
        session.run(
            queryOf(
                """
SELECT COUNT(1)
FROM godkjenning g
WHERE g.vedtaksperiode_id = ?
AND g.godkjent_tidspunkt = ?
""",
                vedtaksperiodeId, godkjentTidspunkt
            ).map { it.int(1) > 0 }.asSingle
        )!!
    }
