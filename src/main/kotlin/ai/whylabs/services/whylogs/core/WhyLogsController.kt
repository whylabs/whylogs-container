package ai.whylabs.services.whylogs.core

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.JsonNodeType
import com.whylogs.core.DatasetProfile
import io.javalin.http.Context
import io.javalin.plugin.openapi.annotations.ContentType
import io.javalin.plugin.openapi.annotations.HttpMethod
import io.javalin.plugin.openapi.annotations.OpenApi
import io.javalin.plugin.openapi.annotations.OpenApiContent
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody
import io.javalin.plugin.openapi.annotations.OpenApiResponse
import org.slf4j.LoggerFactory


internal const val AttributeKey = "jsonObject"
internal val DummyObject = Object()

class WhyLogsController {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val mapper = ObjectMapper()

    private val outputPath = System.getenv("OUTPUT_PATH") ?: "/opt/whylogs/output"

    private val profileManager = WhyLogsProfileManager(outputPath, awsKmsKeyId = System.getenv("AWS_KMS_KEY_ID"))

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
        logger.debug("Request body: {}", body)

        val datasetName = body.get("datasetName").asText("unknown")
        val tags = mutableMapOf("Name" to datasetName)
        val profile = profileManager.getProfile(tags)
        val singleEntry = body.get("single")
        val multipleEntries = body.get("multiple")

        if (singleEntry.isMissingNode && multipleEntries.isMissingNode) {
            return400(ctx, "Missing input data")
        }
        if (!singleEntry.isMissingNode) {
            trackSingle(singleEntry, ctx, profile)
            return
        }
        if (!multipleEntries.isMissingNode) {
            trackMultiple(multipleEntries, ctx, profile)
        }
    }

    private fun trackMultiple(
        multipleEntries: JsonNode,
        ctx: Context,
        profile: DatasetProfile,
    ) {
        val featuresJson = multipleEntries.get("columns")
        val dataJson = multipleEntries.get("data")

        if (!(featuresJson.isArray && featuresJson.all { it.isTextual }) || !(dataJson.isArray && dataJson.all { it.isArray })) {
            return400(ctx, "Malformed input data")
            return
        }
        val features = featuresJson.map { value -> value.textValue() }.toList()
        logger.debug("Track multiple entries. Features: {}", features)
        for (entry in dataJson) {
            for (i in 0..features.size) {
                trackInProfile(profile, features[i], entry.get(i))
            }
        }
    }

    private fun return400(ctx: Context, message: String) {
        ctx.res.status = 400
        ctx.result(message)
    }

    private fun trackSingle(
        inputMap: JsonNode,
        ctx: Context,
        profile: DatasetProfile,
    ) {
        if (!inputMap.isObject) {
            return400(ctx, "InputMap is not an object")
            return
        }

        logger.info("Track single entries. Fields: {}", inputMap.fieldNames().asSequence().toList())
        for (field in inputMap.fields()) {
            trackInProfile(profile, field.key, field.value)
        }
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