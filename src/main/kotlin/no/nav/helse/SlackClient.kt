package no.nav.helse

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import io.ktor.client.HttpClient
import io.ktor.client.request.accept
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.http.ContentType
import io.ktor.http.contentType
import io.ktor.http.userAgent

class SlackClient(private val httpClient: HttpClient, private val accessToken: String) {
    suspend fun postMessage(
        channel: String,
        message: String
    ) = httpClient.post<MessageResponse>("https://slack.com/api/chat.postMessage") {
        userAgent("navikt/spaghet")
        accept(ContentType.Application.Json)
        contentType(ContentType.Application.Json)
        header("Authorization", "Bearer $accessToken")
        body = objectMapper.createObjectNode()
            .put("channel", channel)
            .put("text", message)
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class MessageResponse(
    val ok: Boolean,
    val ts: String
)
