package ai.whylabs.services.whylogs

import ai.whylabs.services.whylogs.core.LogRequest
import ai.whylabs.services.whylogs.core.PubSubEnvelope
import ai.whylabs.services.whylogs.core.WriteProfilesResponse
import ai.whylabs.services.whylogs.core.config.IEnvVars
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.whylogs.core.DatasetProfile
import com.whylogs.core.message.DatasetProfileMessage
import kotlinx.coroutines.delay
import java.io.File
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

private val mapper = jacksonObjectMapper()

class TestClient(val envVars: IEnvVars) {

    private val client = HttpClient.newBuilder().build()

    inline fun withServer(block: () -> Unit) {
        val job = startServer(envVars)

        try {
            block()
        } finally {
            job.stop()
        }
    }

    suspend fun track(request: LogRequest) = request(request, "/logs")
    suspend fun trackPubSub(request: PubSubEnvelope) = request(request, "/pubsubLogs")

    private suspend inline fun <reified T> request(request: T, path: String) {
        val httpRequest = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:${envVars.port}$path"))
            .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(request)))
            .header("X-API-Key", envVars.expectedApiKey)
            .build()
        val response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString())

        val code = response.statusCode()
        if (code != 200) {
            throw RuntimeException(response.body())
        }
        // Lame way of waiting, no synchronous mode yet in the container
        delay(1_000)
    }

    suspend fun writeProfiles(): WriteProfilesResponse {
        val writeRequest = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:${envVars.port}/writeProfiles"))
            .POST(HttpRequest.BodyPublishers.noBody())
            .header("X-API-Key", envVars.expectedApiKey)
            .build()
        val writeResponse = client.send(writeRequest, HttpResponse.BodyHandlers.ofString())

        // Lame way of waiting, no synchronous mode yet in the container
        delay(1_000)
        return mapper.readValue(writeResponse.body())
    }

    fun loadProfiles(paths: List<String>): List<DatasetProfile> {
        return paths.map { path ->
            val pbFile = File(path)
            val profileMessage = DatasetProfileMessage.parseFrom(pbFile.inputStream())
            DatasetProfile.fromProtobuf(profileMessage)
        }
    }
}
