package ai.whylabs.services.whylogs

import ai.whylabs.services.whylogs.core.WhyLogsController
import io.javalin.Javalin
import io.javalin.apibuilder.ApiBuilder.path
import io.javalin.apibuilder.ApiBuilder.post
import io.javalin.plugin.openapi.OpenApiOptions
import io.javalin.plugin.openapi.OpenApiPlugin
import io.javalin.plugin.openapi.ui.SwaggerOptions
import io.swagger.v3.oas.models.info.Info
import org.slf4j.LoggerFactory

data class ErrorResponse(
    val title: String,
    val status: Int,
    val type: String,
    val details: Map<String, String>?
)

fun main(args: Array<String>) {
    val logger = LoggerFactory.getLogger("ai.whylabs.services.whylogs")
    val whylogs = WhyLogsController()
    val app = Javalin.create {
        it.registerPlugin(getConfiguredOpenApiPlugin())
        it.defaultContentType = "application/json"
    }

    app.before("logs", whylogs::preprocess)
    app.routes {
        path("logs") {
            post(whylogs::track)
        }
    }

    app.start(7001)

    logger.info("Check out ReDoc docs at http://localhost:7001/swagger-ui")
}

fun getConfiguredOpenApiPlugin() = OpenApiPlugin(
    OpenApiOptions(
        Info().apply {
            version("1.0")
            description("whylogs API")
        }
    ).apply {
        path("/swagger-docs") // endpoint for OpenAPI json
        swagger(SwaggerOptions("/swagger-ui")) // endpoint for swagger-ui
        defaultDocumentation { doc ->
            doc.json("500", ErrorResponse::class.java)
            doc.json("503", ErrorResponse::class.java)
        }
    }
)
