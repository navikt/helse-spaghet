package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import kotliquery.Row
import kotliquery.Session
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.JsonMessage
import java.util.*
import javax.sql.DataSource

object Util {
    fun JsonNode?.asNullableText(): String? {
        return if (this != null && this.isValueNode && this.isTextual) {
            return this.asText()
        } else null
    }

    fun JsonNode.asUuid() = UUID.fromString(this.asText())

    fun JsonMessage.jsonNode() = objectMapper.readTree(toJson())

    fun <T> DataSource.withSession(f: Session.() -> T) =
        sessionOf(this).use(f)

    fun List<String>.toJson() =
        map { "\"$it\"" }
            .joinToString(
                separator = ", ",
                prefix = "[",
                postfix = "]"
            )

    fun Row.uuid(column: String) = UUID.fromString(string(column))


}