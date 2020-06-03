package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import javax.sql.DataSource

fun DataSource.insertGodkjenning(løsning: GodkjenningLøsning) =
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
                            kommentar
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    løsning.vedtaksperiodeId,
                    løsning.aktørId,
                    løsning.fødselsnummer,
                    løsning.godkjenning.saksbehandlerIdent,
                    løsning.godkjenning.godkjentTidspunkt,
                    løsning.godkjenning.godkjent,
                    løsning.godkjenning.årsak,
                    løsning.godkjenning.kommentar
                ).asUpdateAndReturnGeneratedKey
            )
            løsning.warnings.forEach { warning ->
                transaction.run(
                    queryOf("INSERT INTO warning(godkjenning_ref, tekst) VALUES(?, ?);", godkjenningId, warning)
                        .asUpdate
                )
            }
            løsning.godkjenning.begrunnelser?.forEach { begrunnelse ->
                transaction.run(
                    queryOf("INSERT INTO begrunnelse(godkjenning_ref, tekst) VALUES(?, ?);", godkjenningId, begrunnelse)
                        .asUpdate
                )
            }
        }
    }
