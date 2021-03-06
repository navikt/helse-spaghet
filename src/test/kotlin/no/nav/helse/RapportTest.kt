package no.nav.helse

import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.util.UUID
import kotlin.random.Random

class RapportTest {
    val periodetyper = arrayOf(
        "FØRSTEGANGSBEHANDLING",
        "FORLENGELSE",
        "INFOTRYGDFORLENGELSE",
        "OVERGANG_FRA_IT"
    )

    @Test
    fun genererRapport() {
        Rapport(godkjenninger, "oz").meldinger.forEach { melding ->
            println(melding.tekst)
            melding.tråd.forEach { trådmelding ->
                println("--------")
                println(trådmelding)
            }
            println("========")
        }
    }

    fun godkjenning(index: Any): GodkjenningDto {
        val godkjent = random.nextInt(10) != 0
        return if (godkjent) {
            GodkjenningDto(
                vedtaksperiodeId = UUID.randomUUID(),
                aktørId = UUID.randomUUID().toString(),
                fødselsnummer = UUID.randomUUID().toString(),
                godkjentTidspunkt = LocalDateTime.now(),
                godkjent = godkjent,
                årsak = null,
                kommentar = null,
                periodetype = periodetyper.random(random),
                warnings = warnings.randomEntries(),
                begrunnelse = listOf()
            )
        } else {
            val årsak = årsaker.random(random)
            GodkjenningDto(
                vedtaksperiodeId = UUID.randomUUID(),
                aktørId = UUID.randomUUID().toString(),
                fødselsnummer = UUID.randomUUID().toString(),
                godkjentTidspunkt = LocalDateTime.now(),
                godkjent = godkjent,
                årsak = årsak,
                kommentar = if (random.nextInt(5) == 0) "Dette ser helt forferdelig ut" else null,
                periodetype = periodetyper.random(random),
                warnings = warnings.randomEntries(),
                begrunnelse = if (årsak == årsakOkIInfotrygd) listOf() else  begrunnelser.randomEntries(1)
            )
        }
    }

    fun <T> List<T>.randomEntries(minEntries: Int = 0): List<T> {
        val mutableCopy = toMutableList()
        return 0.until(random.nextInt(size)).map {
            if (mutableCopy.size == 1) return@map mutableCopy.removeAt(0)
            mutableCopy.removeAt(random.nextInt(minEntries, mutableCopy.size))
        }
    }

    private val random = Random.Default
    private val årsakOkIInfotrygd = "Allerede behandlet i infotrygd - riktig vurdering"
    private val årsaker = listOf(
        "Allerede behandlet i infotrygd - feil vurdering og/eller beregning",
        "Feil vurdering og/eller beregning",
        årsakOkIInfotrygd
    )

    private val begrunnelser = listOf(
        "Maksdato beregnet feil",
        "Annet",
        "Arbeidsgiverperiode beregnet feil",
        "Vilkår ikke oppfylt",
        "Vilkår om Lovvalg og medlemskap er ikke oppfylt",
        "Dagsats beregnet feil"
    )

    private val warnings = listOf(
        "Perioden er en direkte overgang fra periode i Infotrygd",
        "Perioden er lagt inn i Infotrygd - men mangler inntektsopplysninger. Fjern perioden fra SP UB hvis du utbetaler via speil.",
        "Simulering kom frem til et annet totalbeløp. Kontroller beløpet til utbetaling"
    )

    private val godkjenninger = 0.rangeTo(500).map(::godkjenning)
}
