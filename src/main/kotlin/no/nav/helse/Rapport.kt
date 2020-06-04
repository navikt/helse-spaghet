package no.nav.helse

class Rapport(
    godkjenningDto: List<GodkjenningDto>
) : Printbar {
    val årsaker: List<Årsak> = godkjenningDto
        .filterNot { it.godkjent }
        .groupBy { it.årsak }
        .map { (årsak, godkjenninger) ->
            Årsak(
                tekst = requireNotNull(årsak),
                antall = godkjenninger.size,
                begrunnelser = godkjenninger
                    .tellTyperBegrunnelser(),
                warnings = godkjenninger.tellTyperWarnings()
            )
        }
    val warnings: List<Warning> = godkjenningDto.tellTyperWarnings()

    fun List<GodkjenningDto>.tellTyperWarnings() = flatMap { it.warnings }
        .groupingBy { it }
        .eachCount()
        .map { (warning, antall) -> Warning(antall, warning) }

    fun List<GodkjenningDto>.tellTyperBegrunnelser() = flatMap { it.begrunnelse }
        .groupingBy { it }
        .eachCount()
        .map { (begrunnelse, antall) ->
            Begrunnelse(
                antall = antall,
                tekst = begrunnelse
            )
        }

    val antallGodkjente = godkjenningDto.count { it.godkjent }
    val antallInfotrygd = godkjenningDto.count { !it.godkjent }

    val godkjenteUtenWarnings = godkjenningDto
        .filter { it.godkjent }
        .count { it.warnings.isEmpty() }

    val avvisteUtenWarnings = godkjenningDto
        .filterNot { it.godkjent }
        .count { it.warnings.isEmpty() }

    val warningsPåGodkjentePerioder = godkjenningDto
        .filter { it.godkjent }
        .tellTyperWarnings()

    override fun tilMelding(): String =
        """*Statistikk over godkjente vedtaksperioder* :information_desk_person:
Siden i går har $antallGodkjente ($godkjenteUtenWarnings uten warnings) vedtaksperioder blitt godkjent og $antallInfotrygd ($avvisteUtenWarnings uten warnings) avvist. Av de avviste vedtaksperiodene er:



*$antallGodkjente godkjente vedtaksperioder*
${warningsPåGodkjentePerioder.formatterWarnings()}
${årsaker.tilMelding("")}

:spaghet:
"""

    class Årsak(
        val tekst: String,
        val antall: Int,
        val begrunnelser: List<Begrunnelse>,
        val warnings: List<Warning>
    ) : Printbar {
        override fun tilMelding(): String {
            val melding = StringBuilder()
            melding.appendln()
            melding.appendln()
            melding.appendln("*$antall avvist på grunn av $tekst*")
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
        val tekst: String
    ) : Printbar {
        override fun tilMelding() = """($antall) $tekst"""
    }

    data class Warning(
        val antall: Int,
        val tekst: String
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
        melding.appendln("```")
    }
    return melding.toString()
}

fun List<Printbar>.tilMelding(prefix: String = " - ") = joinToString("\n") { "${prefix}${it.tilMelding()}" }

interface Printbar {
    fun tilMelding(): String
}
