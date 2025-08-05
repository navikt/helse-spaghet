package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.E2eTestApp.Companion.e2eTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.util.*

class GodkjenningDaoTest {
    @Test
    fun `lagrer informasjon fra godkjenninger`() =
        e2eTest {
            val behandlingId = UUID.randomUUID()
            val løsning =
                Godkjenningsbehov(
                    vedtaksperiodeId = UUID.randomUUID(),
                    aktørId = "aktørId",
                    fødselsnummer = "fødselsnummer",
                    periodetype = "FORLENGELSE",
                    inntektskilde = "EN_ARBEIDSGIVER",
                    løsning =
                        Godkjenningsbehov.Løsning(
                            godkjent = false,
                            saksbehandlerIdent = "Z999999",
                            godkjentTidspunkt = LocalDateTime.now(),
                            årsak = "Annet",
                            begrunnelser = listOf("Begrunnelse", "Begrunnelse2", "Annet"),
                            kommentar = "Feil første fraværsdato",
                        ),
                    utbetalingType = "UTBETALING",
                    refusjonType = "FULL_REFUSJON",
                    saksbehandleroverstyringer = emptyList(),
                    behandlingId = behandlingId,
                )

            dataSource.insertGodkjenning(løsning)

            val godkjenninger = finnGodkjenninger(løsning.vedtaksperiodeId)
            assertEquals(1, godkjenninger.size)
            assertEquals(løsning.løsning.begrunnelser, finnBegrunnelser(godkjenninger.first().id))
            assertFalse(godkjenninger.first().erSaksbehandleroverstyringer)
            assertEquals(emptyList<UUID>(), finnGodkjenningOverstyringer(godkjenninger.first().id))
            assertEquals(behandlingId, godkjenninger.first().behandlingId)
        }

    @Test
    fun `lagrer saksbehandleroverstyringer for godkjenning`() =
        e2eTest {
            val saksbehandleroverstyringer = listOf(UUID.randomUUID())
            val løsning =
                Godkjenningsbehov(
                    vedtaksperiodeId = UUID.randomUUID(),
                    aktørId = "aktørId",
                    fødselsnummer = "fødselsnummer",
                    periodetype = "FORLENGELSE",
                    inntektskilde = "EN_ARBEIDSGIVER",
                    løsning =
                        Godkjenningsbehov.Løsning(
                            godkjent = false,
                            saksbehandlerIdent = "Z999999",
                            godkjentTidspunkt = LocalDateTime.now(),
                            årsak = "Annet",
                            begrunnelser = listOf("Begrunnelse", "Begrunnelse2", "Annet"),
                            kommentar = "Feil første fraværsdato",
                        ),
                    utbetalingType = "UTBETALING",
                    refusjonType = "FULL_REFUSJON",
                    saksbehandleroverstyringer = saksbehandleroverstyringer,
                    behandlingId = UUID.randomUUID(),
                )

            dataSource.insertGodkjenning(løsning)

            val godkjenninger = finnGodkjenninger(løsning.vedtaksperiodeId)
            assertEquals(1, godkjenninger.size)
            assertTrue(godkjenninger.first().erSaksbehandleroverstyringer)
            assertEquals(saksbehandleroverstyringer, finnGodkjenningOverstyringer(godkjenninger.first().id))
        }

    private fun E2eTestApp.finnGodkjenninger(vedtaksperiodeId: UUID) =
        sessionOf(dataSource).use { session ->
            session.run(
                queryOf("SELECT * FROM godkjenning WHERE vedtaksperiode_id=?;", vedtaksperiodeId)
                    .map {
                        TestGodkjenningDto(
                            id = it.long("id"),
                            kommentar = it.stringOrNull("kommentar"),
                            erSaksbehandleroverstyringer = it.boolean("er_saksbehandleroverstyringer"),
                            behandlingId = it.uuid("behandling_id"),
                        )
                    }.asList,
            )
        }

    private fun E2eTestApp.finnBegrunnelser(id: Long) =
        sessionOf(dataSource).use { session ->
            session.run(
                queryOf("SELECT * FROM begrunnelse WHERE godkjenning_ref=?;", id)
                    .map {
                        it.string("tekst")
                    }.asList,
            )
        }

    private fun E2eTestApp.finnGodkjenningOverstyringer(id: Long) =
        sessionOf(dataSource).use { session ->
            session.run(
                queryOf("SELECT * FROM godkjenning_overstyringer WHERE godkjenning_ref=?;", id)
                    .map {
                        it.uuid("overstyring_hendelse_id")
                    }.asList,
            )
        }

    data class TestGodkjenningDto(
        val id: Long,
        val kommentar: String?,
        val erSaksbehandleroverstyringer: Boolean,
        val behandlingId: UUID,
    )
}
