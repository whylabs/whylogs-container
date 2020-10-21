package ai.whylabs.services.whylogs.core

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.JsonNodeType
import com.whylogs.core.DatasetProfile
import io.javalin.http.Context
import io.javalin.plugin.openapi.annotations.*
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.*


internal const val AttributeKey = "jsonObject"
internal val DummyObject = Object()

class WhyLogsController {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val mapper = ObjectMapper()
    private val sessionId = UUID.randomUUID().toString()
    private val sessionTime = Instant.now()

    private val outputPath = System.getenv("OUTPUT_PATH") ?: "/opt/whylogs/output"

    private val profileManager = WhyLogsProfileManager(outputPath)

    fun preprocess(ctx: Context) {
        try {
            val parser = mapper.createParser(ctx.req.inputStream)
            val objNode = parser.readValueAsTree<JsonNode>()
            ctx.attribute(AttributeKey, objNode)
        } catch (e: Exception) {
            logger.warn("Exception: {}", e.message)
        }
    }

    @OpenApi(
        method = HttpMethod.POST,
        summary = "Log a map of feature names and values or an array of data points",
        operationId = "track",
        tags = ["whylogs"],
        requestBody = OpenApiRequestBody(content = [OpenApiContent(type = ContentType.JSON)]),
        responses = [OpenApiResponse("200"), OpenApiResponse("400", description = "Bad or invalid input body")]
    )
    fun track(ctx: Context) {
        val body = ctx.attribute<JsonNode>(AttributeKey)

        if (body == null) {
            return400(ctx, "Missing or invalid body request")
            return
        }

        val datasetName = body.get("datasetName").asText("unknown")
        val tags = mutableMapOf("Name" to datasetName)
        val profile = profileManager.getProfile(tags)
        val singleEntry = body.get("single")
        val multipleEntries = body.get("multiple")

        if (singleEntry.isMissingNode && multipleEntries.isMissingNode) {
            return400(ctx, "Missing input data")

        }
        if (!singleEntry.isMissingNode) {
            trackInputMap(singleEntry, ctx, profile)
            return
        }
        if (!multipleEntries.isMissingNode) {
            val featuresJson = multipleEntries.get("columns")
            val dataJson = multipleEntries.get("data")
            if (!(featuresJson.isArray && featuresJson.all { it.isTextual }) || !(dataJson.isArray && dataJson.all { it.isArray })) {
                return400(ctx, "Malformed input data")
                return
            }
            val features = featuresJson.map { value -> value.textValue() }.toList()
            for (entry in dataJson) {
                for (i in 0..features.size) {
                    trackInProfile(profile, features[i], entry.get(i))
                }
            }
        }
    }

    private fun return400(ctx: Context, message: String) {
        ctx.res.status = 400
        ctx.result(message)
    }

    private fun trackInputMap(
        inputMap: JsonNode,
        ctx: Context,
        profile: DatasetProfile,
    ): Boolean {
        if (!inputMap.isObject) {
            ctx.res.status = 400
            ctx.result("InputMap is not an object")
            return true
        }
        for (field in inputMap.fields()) {
            trackInProfile(profile, field.key, field.value)
        }
        return false
    }

    private fun trackInProfile(
        profile: DatasetProfile,
        featureName: String,
        value: JsonNode,
    ) {
        when (value.nodeType) {
            JsonNodeType.ARRAY, JsonNodeType.BINARY, JsonNodeType.POJO, JsonNodeType.OBJECT -> {
                profile.track(
                    featureName,
                    DummyObject
                )
            }
            JsonNodeType.BOOLEAN -> profile.track(featureName, value.booleanValue())
            JsonNodeType.NUMBER -> profile.track(featureName, value.numberValue())
            JsonNodeType.STRING -> profile.track(featureName, value.textValue())
            JsonNodeType.NULL -> profile.track(featureName, null)
            JsonNodeType.MISSING -> {
            }
            else -> {
            }
        }
    }
}