package no.nav.helse.ventetilstand

import no.nav.helse.rapids_rivers.JsonMessage
import java.util.UUID

internal class Hendelse (
    internal val id: UUID,
    internal val hendelse: String
)

internal val JsonMessage.hendelse get() = Hendelse(
    id = UUID.fromString(this["@id"].asText()),
    hendelse = this.toJson()
)

