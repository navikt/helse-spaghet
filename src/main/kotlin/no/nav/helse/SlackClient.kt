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
        text: String,
        threadTs: String? = null
    ) = httpClient.post<MessageResponse>("https://slack.com/api/chat.postMessage") {
        userAgent("navikt/spaghet")
        accept(ContentType.Application.Json.withParameter("charset", "UTF-8"))
        contentType(ContentType.Application.Json.withParameter("charset", "UTF-8"))
        header("Authorization", "Bearer $accessToken")

        val message = objectMapper.createObjectNode()
            .put("channel", channel)
            .put("text", text)
        threadTs?.also {
            message.put("thread_ts", it)
        }
        body = message

    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class MessageResponse(
    val ok: Boolean,
    val ts: String
)
