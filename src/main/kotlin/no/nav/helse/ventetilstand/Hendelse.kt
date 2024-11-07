package no.nav.helse.ventetilstand

import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageProblems
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDate
import java.util.UUID

internal class Hendelse (
    internal val id: UUID,
    internal val hendelse: String
)

internal val JsonMessage.hendelse get() = Hendelse(
    id = UUID.fromString(this["@id"].asText()),
    hendelse = this.toJson()
)

