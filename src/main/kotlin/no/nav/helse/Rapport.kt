package no.nav.helse

data class Melding(
    val tekst: String,
    val tråd: List<String> = emptyList()
)

class Rapport(
    godkjenningDto: List<GodkjenningDto>
) {
    private val årsaker: List<Årsak> = godkjenningDto
        .filterNot { it.godkjent }
        .groupBy { it.årsak }
        .map { (årsak, godkjenninger) ->
            Årsak(
                tekst = requireNotNull(årsak),
                antall = godkjenninger.size,
                antallUtenWarnings = godkjenninger.count { it.warnings.isEmpty() },
                begrunnelser = godkjenninger.tellTyperBegrunnelser(),
                warnings = godkjenninger.tellTyperWarnings(),
                kommentarer = godkjenninger.mapNotNull { it.kommentar }.filter { it.isNotBlank() }
            )
        }

    private fun List<GodkjenningDto>.tellTyperWarnings() = flatMap { it.warnings }
        .groupingBy { it }
        .eachCount()
        .map { (warning, antall) -> Warning(antall, warning) }

    private fun List<GodkjenningDto>.tellTyperBegrunnelser() = flatMap { it.begrunnelse }
        .groupingBy { it }
        .eachCount()
        .map { (begrunnelse, antall) ->
            Begrunnelse(
                antall = antall,
                tekst = begrunnelse
            )
        }

    private val antallGodkjente = godkjenningDto.count { it.godkjent }
    private val antallAvvist = godkjenningDto.count { !it.godkjent }

    private val godkjenteUtenWarnings = godkjenningDto
        .filter { it.godkjent }
        .count { it.warnings.isEmpty() }

    private val avvisteUtenWarnings = godkjenningDto
        .filterNot { it.godkjent }
        .count { it.warnings.isEmpty() }

    private val warningsPåGodkjentePerioder = godkjenningDto
        .filter { it.godkjent }
        .tellTyperWarnings()

    private val startmelding = StringBuilder().let {
        it.appendln("*Statistikk over godkjente vedtaksperioder* :information_desk_person:")
        it.appendln("Siden i går er $antallGodkjente godkjent ($godkjenteUtenWarnings uten warnings)")
        it.appendln("og $antallAvvist avvist ($avvisteUtenWarnings uten warnings)")
        it.append("Av de avviste vedtaksperiodene er:")
        Melding(tekst = it.toString())
    }

    private val godkjentemelding = StringBuilder().let {
        it.appendln("*$antallGodkjente godkjente vedtaksperioder*")
        it.append(warningsPåGodkjentePerioder.formatterWarnings())
        Melding(tekst = it.toString())
    }

    private val årsaksmeldinger = årsaker.map { Melding(it.tilMelding(), it.kommentarer) }

    val meldinger = listOf(startmelding, godkjentemelding) + årsaksmeldinger + Melding(":spaghet:")

    class Årsak(
        private val tekst: String,
        private val antall: Int,
        private val antallUtenWarnings: Int,
        private val begrunnelser: List<Begrunnelse>,
        private val warnings: List<Warning>,
        val kommentarer: List<String>
    ) : Printbar {
        override fun tilMelding(): String {
            val melding = StringBuilder()
            melding.appendln("*$antall avvist på grunn av $tekst ($antallUtenWarnings uten warnings)*")
            if (begrunnelser.isNotEmpty()) {
                melding.appendln()
                melding.appendln(":memo: Vedtaksperiodene hadde følgende begrunnelser:")
                melding.appendln("```")
                melding.appendln(begrunnelser.sortedByDescending { it.antall }.tilMelding())
                melding.appendln("```")
            }
            melding.append(warnings.formatterWarnings())
            return melding.toString()
        }
    }

    class Begrunnelse(
        val antall: Int,
        private val tekst: String
    ) : Printbar {
        override fun tilMelding() = """($antall) $tekst"""
    }

    data class Warning(
        val antall: Int,
        private val tekst: String
    ) : Printbar {
        override fun tilMelding() = """($antall) $tekst"""
    }
}

fun List<Rapport.Warning>.formatterWarnings(): String {
    val melding = StringBuilder()
    if (isNotEmpty()) {
        melding.appendln()
        melding.appendln(":warning: Vedtaksperiodene hadde følgende warnings:")
        melding.appendln("```")
        melding.appendln(sortedByDescending { it.antall }.tilMelding())
        melding.append("```")
    }
    return melding.toString()
}

fun List<Printbar>.tilMelding(prefix: String = " - ") = joinToString("\n") { "${prefix}${it.tilMelding()}" }

interface Printbar {
    fun tilMelding(): String
}
