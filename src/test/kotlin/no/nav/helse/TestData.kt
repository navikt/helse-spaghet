package no.nav.helse

import no.nav.helse.Util.toJson
import java.time.LocalDateTime
import java.util.*

object TestData {
    fun Annullering.toJson(): String =
        """{
            "@event_name": "annullering",
            "saksbehandler": {"oid": "$saksbehandler"},
            "fagsystemId": "$fagsystemId",
            "begrunnelser": ${begrunnelser.toJson()},
            "kommentar": ${kommentar?.let { "\"$it\"" }},
            "@opprettet": "$opprettet"
         }""".trimMargin()

    val annullering = Annullering(
        saksbehandler = UUID.randomUUID(),
        fagsystemId = "ABCD12345",
        begrunnelser = listOf(),
        kommentar = null,
        opprettet = LocalDateTime.parse("2021-03-15T16:59:58")
    )

    fun Annullering.fagsystemId(it: String) = copy(fagsystemId = it)
    fun Annullering.kommentar(it: String) = copy(kommentar = it)
    fun Annullering.begrunnelse(it: String) = copy(begrunnelser = begrunnelser + it)
}