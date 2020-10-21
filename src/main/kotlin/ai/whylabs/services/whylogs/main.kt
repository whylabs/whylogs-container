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

fun main() {
    val logger = LoggerFactory.getLogger("ai.whylabs.services.whylogs")
    val whylogs = WhyLogsController()
    val app = Javalin.create {
        it.registerPlugin(getConfiguredOpenApiPlugin())
        it.defaultContentType = "application/json"
        it.showJavalinBanner = false
    }

    app.before("logs", whylogs::preprocess)
    app.routes {
        path("logs") {
            post(whylogs::track)
        }
    }

    val port = System.getenv("PORT")?.toInt() ?: 8080
    app.start(port)

    logger.info("Checkout Swagger UI at http://localhost:8080/swagger-ui")
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
    }
)
