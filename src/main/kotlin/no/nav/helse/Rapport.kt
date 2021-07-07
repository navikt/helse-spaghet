package no.nav.helse

data class Melding(
    val tekst: String,
    val tråd: List<String> = emptyList()
)

class Rapport(
    godkjenningDto: List<GodkjenningDto>,
    miljø: String
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
                periodetyper = godkjenninger.tellPeriodetyper(),
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

    private fun List<GodkjenningDto>.tellPeriodetyper() =
        groupBy { it.periodetype }
        .map { (periodetype, godkjenninger) ->
            Periodetype(
                antall = godkjenninger.size,
                antallUtenWarnings = godkjenninger.count { it.warnings.isEmpty() },
                type = periodetype ?: "ukjent"
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
        it.appendLine("*Hei! Statistikk over behandlede vedtaksperioder _i ${miljø}_* :information_desk_person:")
        it.appendLine("I går ble det gjort $antallGodkjente godkjenninger ($godkjenteUtenWarnings uten warnings)")
        it.appendLine("og $antallAvvist avvisninger ($avvisteUtenWarnings uten warnings)")
        it.append("Av de avviste vedtaksperiodene er:")
        Melding(tekst = it.toString())
    }

    private val godkjentemelding = StringBuilder().let { stringBuilder ->
        stringBuilder.appendLine("*$antallGodkjente godkjente vedtaksperioder*")
        stringBuilder.appendLine(godkjenningDto.filter { it.godkjent }.tellPeriodetyper().tilMelding(""))
        stringBuilder.append(warningsPåGodkjentePerioder.formatterWarnings())
        Melding(tekst = stringBuilder.toString())
    }

    private val årsaksmeldinger = årsaker.map { Melding(it.tilMelding(), it.kommentarer) }

    val meldinger = listOf(startmelding, godkjentemelding) + årsaksmeldinger + Melding(":spaghet:")

    class Årsak(
        private val tekst: String,
        private val antall: Int,
        private val antallUtenWarnings: Int,
        private val begrunnelser: List<Begrunnelse>,
        private val periodetyper: List<Periodetype>,
        private val warnings: List<Warning>,
        val kommentarer: List<String>
    ) : Printbar {
        override fun tilMelding(): String {
            val melding = StringBuilder()
            melding.appendLine("*$antall avvist på grunn av $tekst ($antallUtenWarnings uten warnings)*")
            melding.appendLine(periodetyper.tilMelding(""))
            if (begrunnelser.isNotEmpty()) {
                melding.appendLine()
                melding.appendLine(":memo: Vedtaksperiodene hadde følgende begrunnelser:")
                melding.appendLine("```")
                melding.appendLine(begrunnelser.sortedByDescending { it.antall }.tilMelding())
                melding.appendLine("```")
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

    data class Periodetype(
        private val antall: Int,
        private val antallUtenWarnings: Int,
        private val type: String
    ) : Printbar {
        override fun tilMelding() = """${type.replace("_", " ").toLowerCase().capitalize()}: $antall ($antallUtenWarnings uten warnings)"""
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
        melding.appendLine()
        melding.appendLine(":warning: Vedtaksperiodene hadde følgende warnings:")
        melding.appendLine("```")
        melding.appendLine(sortedByDescending { it.antall }.tilMelding())
        melding.append("```")
    }
    return melding.toString()
}

fun List<Printbar>.tilMelding(prefix: String = " - ") = joinToString("\n") { "${prefix}${it.tilMelding()}" }

interface Printbar {
    fun tilMelding(): String
}
