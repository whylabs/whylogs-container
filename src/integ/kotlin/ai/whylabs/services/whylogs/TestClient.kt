package ai.whylabs.services.whylogs

import ai.whylabs.services.whylogs.core.LogRequest
import ai.whylabs.services.whylogs.core.PubSubEnvelope
import ai.whylabs.services.whylogs.core.WriteProfilesResponse
import ai.whylabs.services.whylogs.core.config.IEnvVars
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.github.michaelbull.retry.policy.RetryPolicy
import com.github.michaelbull.retry.policy.binaryExponentialBackoff
import com.github.michaelbull.retry.policy.limitAttempts
import com.github.michaelbull.retry.policy.plus
import com.github.michaelbull.retry.retry
import com.whylogs.core.DatasetProfile
import com.whylogs.core.message.DatasetProfileMessage
import io.swagger.models.HttpMethod
import kotlinx.coroutines.delay
import org.apache.commons.io.FileUtils
import java.io.File
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

private val mapper = jacksonObjectMapper()

val policy: RetryPolicy<Throwable> = binaryExponentialBackoff(50, 5000) + limitAttempts(5)

class TestClient(val envVars: IEnvVars) {

    private val client = HttpClient.newBuilder().build()

    suspend inline fun withServer(block: () -> Unit) {
        val job = startServer(envVars)

        retry(policy) {
            this.healthCheck()
        }

        try {
            deleteLocalProfiles()
            block()
        } finally {
            job.stop()
            deleteLocalProfiles()
        }
    }

    fun deleteLocalProfiles() {
        try {
            val file = File("./$profileRoot")
            val profiles = FileUtils.listFiles(file, arrayOf("bin"), true)
            println("Deleting files $profiles")
            FileUtils.deleteDirectory(file)
        } catch (t: Throwable) {
        }
    }

    suspend fun track(request: LogRequest) = request(request, "/logs")
    suspend fun trackPubSub(request: PubSubEnvelope) = request(request, "/pubsubLogs")
    suspend fun healthCheck() = request(Unit, "/health", HttpMethod.GET)

    private suspend inline fun <reified T> request(request: T, path: String, method: HttpMethod = HttpMethod.POST) {
        val httpRequest = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:${envVars.port}$path")).apply {
                if (method == HttpMethod.POST) {
                    POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(request)))
                } else {
                    GET()
                }
            }
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
