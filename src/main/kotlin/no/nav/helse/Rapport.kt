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

    override fun tilMelding(): String =
"""Statistikk over godkjente saker
Siden i går har $antallGodkjente saker blitt godkjent og $antallInfotrygd avvist. Av de avviste vedtaksperiodene er
${årsaker.tilMelding()}
"""

    class Årsak(
        val tekst: String,
        val antall: Int,
        val begrunnelser: List<Begrunnelse>,
        val warnings: List<Warning>
    ) : Printbar {
        override fun tilMelding() =
            """$antall avvist på grunn av $tekst:
                | ${begrunnelser.tilMelding()}
                | og warnings:
                | ${warnings.tilMelding()}
            """.trimMargin()
    }

    class Begrunnelse(
        val antall: Int,
        val tekst: String
    ) : Printbar {
        override fun tilMelding() = """$antall: $tekst"""
    }

    data class Warning(
        val antall: Int,
        val tekst: String
    ) : Printbar{
        override fun tilMelding() = """$antall: $tekst"""
    }
}

fun List<Printbar>.tilMelding() = joinToString("\n") { it.tilMelding() }

interface Printbar {
    fun tilMelding(): String
}
