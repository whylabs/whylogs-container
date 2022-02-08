package ai.whylabs.services.whylogs.core

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.exc.MismatchedInputException
import io.javalin.http.Context
import io.javalin.http.UnauthorizedResponse
import io.javalin.plugin.openapi.annotations.ContentType
import io.javalin.plugin.openapi.annotations.HttpMethod
import io.javalin.plugin.openapi.annotations.OpenApi
import io.javalin.plugin.openapi.annotations.OpenApiContent
import io.javalin.plugin.openapi.annotations.OpenApiParam
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody
import io.javalin.plugin.openapi.annotations.OpenApiResponse
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory

private const val apiKeyHeader = "X-API-Key"

const val SegmentTagPrefix = "whylogs.tag."
const val DatasetIdTag = "datasetId"
const val OrgIdTag = "orgId"

class WhyLogsController(
    private val envVars: EnvVars = EnvVars(),
    period: String = envVars.period,
    private val profileManager: WhyLogsProfileManager = WhyLogsProfileManager(period = period),
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun preprocess(ctx: Context) {
        // This shouldn't actually happen. Swagger will take care of the validation.
        val apiKey = ctx.header(apiKeyHeader)
            ?: throw IllegalArgumentException("Missing api key in request")

        if (apiKey != envVars.expectedApiKey) {
            throw UnauthorizedResponse("Invalid API key")
        }
    }

    fun after(ctx: Context) {
        profileManager.mergePending()
    }

    @OpenApi(
        headers = [OpenApiParam(name = apiKeyHeader, required = true)],
        method = HttpMethod.POST,
        summary = "Log a map of feature names and values or an array of data points",
        operationId = "track",
        tags = ["whylogs"],
        requestBody = OpenApiRequestBody(
            content = [OpenApiContent(type = ContentType.JSON)],
            description = """
Pass the input in single entry format (a JSON object) or a multiple entry format.
* Set `single` key if you're passing a single data point with multiple features
* Set `multiple` key if you're passing multiple data at once. Here are the required fields:
  * `columns`: specify an `array` of features
  * `data`: array of actual data points
Example:
```
{
  "datasetId": "demo-model",
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
  "datasetId": "demo-model",
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
"""
        ),
        responses = [OpenApiResponse("200"), OpenApiResponse("400", description = "Bad or invalid input body")]
    )
    fun track(ctx: Context) {
        try {
            val request = ctx.bodyStreamAsClass(LogRequest::class.java)

            if (request.single == null && request.multiple == null) {
                return400(ctx, "Missing input data, must supply either a `single` or `multiple` field.")
                return
            }

            // Namespacing hack right now. Whylogs doesn't care about tag names but we want to avoid collisions between
            // user supplied tags and our own internal tags that occupy the same real estate so we prefix user tags.
            val prefixedTags = if (envVars.writer == WriterTypes.S3) request.tags else request.tags?.mapKeys { (key) ->
                "$SegmentTagPrefix$key"
            }

            val processedRequest = request.copy(tags = prefixedTags)

            runBlocking {
                profileManager.enqueue(processedRequest)
            }
        } catch (t: MismatchedInputException) {
            logger.warn("Invalid request format", t)
            throw IllegalArgumentException("Invalid request format", t)
        } catch (t: JsonParseException) {
            logger.warn("Invalid request format", t)
            throw IllegalArgumentException("Invalid request format", t)
        } catch (t: Throwable) {
            logger.error("Error handling request", t)
            throw t
        }
    }

    private fun return400(ctx: Context, message: String) {
        ctx.res.status = 400
        ctx.result(message)
    }
}

data class LogRequest(
    val datasetId: String,
    val tags: Map<String, String>?,
    val single: Map<String, Any>?,
    val multiple: MultiLog?,
)

data class MultiLog(
    val columns: List<String>,
    val data: List<List<Any>>
)
