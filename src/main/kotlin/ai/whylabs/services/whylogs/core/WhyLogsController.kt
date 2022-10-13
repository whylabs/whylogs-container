package ai.whylabs.services.whylogs.core

import ai.whylabs.services.whylogs.core.config.EnvVars
import ai.whylabs.services.whylogs.core.config.IEnvVars
import ai.whylabs.services.whylogs.core.config.WriterTypes
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.exc.MismatchedInputException
import com.fasterxml.jackson.module.kotlin.MissingKotlinParameterException
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.javalin.http.Context
import io.javalin.http.UnauthorizedResponse
import io.javalin.plugin.openapi.annotations.ContentType
import io.javalin.plugin.openapi.annotations.HttpMethod
import io.javalin.plugin.openapi.annotations.OpenApi
import io.javalin.plugin.openapi.annotations.OpenApiContent
import io.javalin.plugin.openapi.annotations.OpenApiParam
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody
import io.javalin.plugin.openapi.annotations.OpenApiResponse
import io.swagger.v3.oas.annotations.media.Schema
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.util.Base64

private const val apiKeyHeader = "X-API-Key"

const val SegmentTagPrefix = "whylogs.tag."
const val DatasetIdTag = "datasetId"
const val OrgIdTag = "orgId"

class WhyLogsController(
    private val envVars: IEnvVars = EnvVars.instance,
    private val profileManager: WhyLogsProfileManager = WhyLogsProfileManager(envVars = envVars),
    private val debugInfo: DebugInfoManager = DebugInfoManager.instance

) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val mapper = jacksonObjectMapper()

    fun preprocess(ctx: Context) {
        val apiKey = ctx.header(apiKeyHeader)?.trim()

        if (apiKey != envVars.expectedApiKey) {
            logger.warn("Dropping request because of invalid API key")
            throw UnauthorizedResponse("Invalid API key")
        }
    }

    fun after(ctx: Context) {
        runBlocking {
            profileManager.mergePending()
        }
    }

    @OpenApi(
        headers = [OpenApiParam(name = apiKeyHeader, required = true)],
        method = HttpMethod.POST,
        summary = "Log Data",
        description = "Log a map of feature names and values or an array of data points",
        operationId = "track",
        tags = ["whylogs"],
        requestBody = OpenApiRequestBody(
            content = [OpenApiContent(from = LogRequest::class, type = ContentType.JSON)],
            description = """
Pass the input in single entry format or a multiple entry format.
- Set `single` key if you're passing a single data point with multiple features
- Set `multiple` key if you're passing multiple data at once.
The `multiple` format is is compatible with Pandas JSON output:
```
import pandas as pd
cars = {'Brand': ['Honda Civic','Toyota Corolla','Ford Focus','Audi A4'],
        'Price': [22000,25000,27000,35000] }
df = pd.DataFrame(cars, columns = ['Brand', 'Price'])
df.to_json(orient="split") # this is the value of `multiple`
```
Here is an example from the output above
```
{
    "datasetId": "demo-model",
    "timestamp": 1648162494947,
    "tags": {
        "tag1": "value1"
    },
    "multiple": {
        "columns": [
            "Brand",
            "Price"
        ],
        "data": [
            [ "Honda Civic", 22000 ],
            [ "Toyota Corolla", 25000 ],
            [ "Ford Focus", 27000 ],
            [ "Audi A4", 35000 ]
        ]
    }
}
```
"""
        ),
        responses = [OpenApiResponse("200"), OpenApiResponse("400", description = "Bad or invalid input body")]
    )
    fun track(ctx: Context) {
        try {
            val request = ctx.bodyStreamAsClass(LogRequest::class.java)
            return trackLogRequest(request)
        } catch (t: MismatchedInputException) {
            logger.warn("Invalid request format", t)
            throw IllegalArgumentException("Invalid request format", t)
        } catch (t: JsonParseException) {
            logger.warn("Invalid request format", t)
            throw IllegalArgumentException("Invalid request format", t)
        }
    }

    private fun trackLogRequest(request: LogRequest) {
        if (request.single == null && request.multiple == null) {
            throw IllegalArgumentException("Missing input data, must supply either a `single` or `multiple` field.")
        }

        try {
            // Namespacing hack right now. Whylogs doesn't care about tag names but we want to avoid collisions between
            // user supplied tags and our own internal tags that occupy the same real estate so we prefix user tags.
            val prefixedTags = if (envVars.writer == WriterTypes.S3) request.tags else request.tags?.mapKeys { (key) ->
                "$SegmentTagPrefix$key"
            }

            val processedRequest = request.copy(tags = prefixedTags)

            runBlocking {
                profileManager.handle(processedRequest)
                debugInfo.send(DebugInfoMessage.RestLogCalledMessage())
            }
        } catch (t: Throwable) {
            logger.error("Error handling request", t)
            throw t
        }
    }

    @OpenApi(
        headers = [OpenApiParam(name = apiKeyHeader, required = true)],
        method = HttpMethod.POST,
        summary = "Write Profiles",
        description = "Force the container to write out the pending profiles via whatever method it's configured for.",
        operationId = "writeProfiles",
        tags = ["whylogs"],
        responses = [
            OpenApiResponse("200", content = [OpenApiContent(from = WriteProfilesResponse::class)]),
            OpenApiResponse("500", description = "Something unexpected went wrong.")
        ]
    )
    fun writeProfiles(ctx: Context) {
        return runBlocking {
            val result = try {
                profileManager.writeOutProfiles()
            } catch (t: Throwable) {
                logger.error("Error writing profiles", t)
                throw t
            }

            val response = WriteProfilesResponse(profilesWritten = result.profilesWritten, profilePaths = result.profilePaths)
            ctx.json(response)
        }
    }

    @OpenApi(
        headers = [OpenApiParam(name = apiKeyHeader, required = true)],
        method = HttpMethod.POST,
        summary = "Log Debug Info",
        description = "Trigger debugging info to be logged.",
        operationId = "logDebugInfo",
        tags = ["whylogs"],
        responses = [
            OpenApiResponse("200"),
            OpenApiResponse("500", description = "Something unexpected went wrong.")
        ]
    )
    fun logDebugInfo(ctx: Context) = runBlocking { debugInfo.send(DebugInfoMessage.LogMessage) }

    @OpenApi(
        headers = [OpenApiParam(name = apiKeyHeader, required = true)],
        method = HttpMethod.POST,
        summary = "Track pub/sub messages",
        description = "Decode base64 encoded pub/sub message and track them",
        operationId = "trackPubSubMessage",
        tags = ["whylogs"],
        requestBody = OpenApiRequestBody(
            content = [OpenApiContent(from = PubSubEnvelope::class, type = ContentType.JSON)],
            description = """
                A Google Pub\Sub interface to tracking data. Does the same thing as /track except
                it consumes a message in the format that Pub\Sub uses.
"""
        ),
        responses = [
            OpenApiResponse("200"),
            OpenApiResponse("500", description = "Something unexpected went wrong.")
        ]
    )
    fun trackMessage(ctx: Context) {
        val encodedMessageData = try {
            // Convert to JSON object
            ctx.bodyStreamAsClass(PubSubEnvelope::class.java).message.data
        } catch (t: MismatchedInputException) {
            logger.warn("Invalid request format", t)
            throw IllegalArgumentException("Invalid request format", t)
        } catch (t: JsonParseException) {
            logger.warn("Invalid request format", t)
            throw IllegalArgumentException("Invalid request format", t)
        }

        val decodedMessageBytes = try {
            val decoder = Base64.getDecoder()
            decoder.decode(encodedMessageData)
        } catch (t: IllegalArgumentException) {
            logger.warn("pubsub message contained invalid base 64")
            throw IllegalArgumentException("pubsub message contained invalid base 64", t)
        }

        try {
            // Create LogRequest object from string
            val logRequest: LogRequest = mapper.readValue(decodedMessageBytes)
            return trackLogRequest(logRequest)
        } catch (t: MissingKotlinParameterException) {
            logger.warn("Couldn't decode the pubsub message", t)
            throw IllegalArgumentException(
                "Couldn't decode the pubsub message. Should have been a JSON encoded LogRequest payload, like the /track API consumes.",
                t
            )
        } catch (t: JsonParseException) {
            logger.warn("Couldn't decode the pubsub message", t)
            throw IllegalArgumentException(
                "Couldn't decode the pubsub message. Should have been a JSON encoded LogRequest payload, like the /track API consumes.",
                t
            )
        }
    }
}

@Schema(description = "Response for writing out profiles.")
data class WriteProfilesResponse(
    @Schema(description = "The amount of profiles that were written out.", example = "2")
    val profilesWritten: Int,
    @Schema(
        description = "The paths of the profiles that were written if they exist. Some writers may not write profiles to anyplace that can be described as a path.",
        example = """["s3://bucket/path/profile.bin"]"""
    )
    val profilePaths: List<String>
)

data class LogRequest(
    @Schema(example = "model-2")
    val datasetId: String,
    val timestamp: Long? = null,
    @Schema(example = """{"city": "Seattle", "job":"SDE"}""")
    val tags: Map<String, String>?,
    @Schema(description = "Key/value pairs of col/data.", example = """{"col1": 1, "col2":"value"}""")
    val single: Map<String, Any>?,
    val multiple: MultiLog?,
)

data class MultiLog(
    @Schema(example = """["column1", "column2"]""")
    val columns: List<String>,
    @Schema(example = """[["column1Value1", 1.0], ["column1Value2", 2.0]]""")
    val data: List<List<Any>>
)

@Schema(description = "Represents a single Google Cloud pub/sub message.")
@JsonIgnoreProperties(ignoreUnknown = true)
data class PubSubEnvelope(
    @Schema(description = "Envelope containing all metadata from pubsub push endpoint request")
    val message: Message,
    @Schema(
        description = "Key value object containing subscription name",
        example = "projects/myproject/subscriptions/mysubscription"
    )
    val subscription: String
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class Message(
    @Schema(example = """{"key":"value"}""")
    val attributes: Map<String, String>? = null,
    @Schema(
        description = "The message data field. If this field is empty, the message must contain at least one attribute.",
        example = "ewogICAgImRhdGFzZXRJZCI6ICJkZW1vLW1vZGVsIiwKICAgICJ0aW1lc3RhbXAiOiAxNjQ4MTYyNDk0OTQ3LAogICAgInRhZ3MiOiB7CiAgICAgICAgInRhZzEiOiAidmFsdWUxIgogICAgfSwKICAgICJtdWx0aXBsZSI6IHsKICAgICAgICAiY29sdW1ucyI6IFsKICAgICAgICAgICAgIkJyYW5kIiwKICAgICAgICAgICAgIlByaWNlIgogICAgICAgIF0sCiAgICAgICAgImRhdGEiOiBbCiAgICAgICAgICAgIFsgIkhvbmRhIENpdmljIiwgMjIwMDAgXSwKICAgICAgICAgICAgWyAiVG95b3RhIENvcm9sbGEiLCAyNTAwMCBdLAogICAgICAgICAgICBbICJGb3JkIEZvY3VzIiwgMjcwMDAgXSwKICAgICAgICAgICAgWyAiQXVkaSBBNCIsIDM1MDAwIF0KICAgICAgICBdCiAgICB9Cn0K"
        )
    val data: String,
    @Schema(example = "123")
    val messageId: String,
    @Schema(
        description = "A timestamp in RFC3339 UTC 'Zulu' format, with nanosecond resolution and up to nine fractional digits",
        example = "2014-10-02T15:01:23Z"
        )
    val publishTime: String,
    @Schema(description = "If non-empty, identifies related messages for which publish order should be respected")
    val orderingKey: String?
)
