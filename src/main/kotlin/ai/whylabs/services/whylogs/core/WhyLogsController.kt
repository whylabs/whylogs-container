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

    private val profileManager = WhyLogsProfileManager(
        outputPath,
        awsKmsKeyId = System.getenv("AWS_KMS_KEY_ID"),
        period = System.getenv("WHYLOGS_PERIOD"),
    )

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
        requestBody = OpenApiRequestBody(content = [OpenApiContent(type = ContentType.JSON)], description = """
Pass the input in single entry format (a JSON object) or a multiple entry format.
* Set `single` key if you're passing a single data point with multiple features
* Set `multiple` key if you're passing multiple data at once. Here are the required fields:
  * `columns`: specify an `array` of features
  * `data`: array of actual data points
Example: 
```
{
  "datasetName": "demo-model",
  "tags": {
    "tagKey": "tagValue"
  },
  "single": {
    "feature1": "test",
    "feature2": 1,
    "feature3": 1.0,
    "feature4": true
  }
}
```

Passing multiple data points. The data is compatible with Pandas JSON output:
```
import pandas as pd

cars = {'Brand': ['Honda Civic','Toyota Corolla','Ford Focus','Audi A4'],
        'Price': [22000,25000,27000,35000]
        }

df = pd.DataFrame(cars, columns = ['Brand', 'Price'])
df.to_json(orient="split")
```

Here is an example from the output above
```json
{
  "datasetName": "demo-model",
  "tags": {
    "tag1": "value1"
  },
  "multiple": {
    "columns": [
      "Brand",
      "Price"
    ],
    "data": [
      [
        "Honda Civic",
        22000
      ],
      [
        "Toyota Corolla",
        25000
      ],
      [
        "Ford Focus",
        27000
      ],
      [
        "Audi A4",
        35000
      ]
    ]
  }
}
```
"""),
        responses = [OpenApiResponse("200"), OpenApiResponse("400", description = "Bad or invalid input body")]
    )
    fun track(ctx: Context) {
        val body = ctx.attribute<JsonNode>(AttributeKey)

        if (body == null) {
            return400(ctx, "Missing or invalid body request")
            return
        }
        logger.debug("Request body: {}", body)

        val inputDatasetName = body.get("datasetName")?.textValue()
        val datasetName = if (inputDatasetName.isNullOrBlank()) "default" else inputDatasetName
        val jsonTags = body.get("tags")
        val tags = mutableMapOf("Name" to datasetName)
        if (jsonTags?.isObject == true) {
            for (tag in jsonTags.fields()) {
                tag.value.textValue()?.let { tags.putIfAbsent(tag.key, it) }
            }
        } else if (jsonTags?.isNull == false) {
            logger.warn("Tags field is not a mapping. Ignoring tagging")
        }

        val profile = profileManager.getProfile(tags)
        val singleEntry = body.get("single")
        val multipleEntries = body.get("multiple")

        if (singleEntry?.isObject != true && multipleEntries?.isObject != true) {
            return400(ctx, "Missing input data")
        }
        if (singleEntry?.isObject == true) {
            trackSingle(singleEntry, ctx, profile)
            return
        }
        if (multipleEntries?.isObject == true) {
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
            for (i in features.indices) {
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

        logger.debug("Track single entries. Fields: {}", inputMap.fieldNames().asSequence().toList())
        for (field in inputMap.fields()) {
            trackInProfile(profile, field.key, field.value)
        }
    }

    private fun trackInProfile(
        profile: DatasetProfile,
        featureName: String,
        value: JsonNode?,
    ) {
        when (value?.nodeType) {
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