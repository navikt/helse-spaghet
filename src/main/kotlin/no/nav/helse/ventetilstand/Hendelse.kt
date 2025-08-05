package no.nav.helse.ventetilstand

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import java.util.*

internal class Hendelse(
    internal val id: UUID,
    internal val hendelse: String,
)

internal val JsonMessage.hendelse get() =
    Hendelse(
        id = UUID.fromString(this["@id"].asText()),
        hendelse = this.toJson(),
    )
