package ai.whylabs.services.whylogs

import ai.whylabs.services.whylogs.core.EnvVars
import ai.whylabs.services.whylogs.core.WhyLogsController
import io.javalin.Javalin
import io.javalin.apibuilder.ApiBuilder.path
import io.javalin.apibuilder.ApiBuilder.post
import io.javalin.plugin.openapi.OpenApiOptions
import io.javalin.plugin.openapi.OpenApiPlugin
import io.javalin.plugin.openapi.ui.SwaggerOptions
import io.swagger.v3.oas.models.info.Info
import org.slf4j.LoggerFactory
import kotlin.system.exitProcess

private val logger = LoggerFactory.getLogger("ai.whylabs.services.whylogs")
fun main(): Unit = try {
    val whylogs = WhyLogsController()

    Javalin.create {
        it.registerPlugin(getConfiguredOpenApiPlugin())
        it.defaultContentType = "application/json"
        it.showJavalinBanner = false
    }.apply {
        Runtime.getRuntime().addShutdownHook(Thread { stop() })
        before("logs", whylogs::preprocess)
        routes {
            path("logs") {
                post(whylogs::track)
            }
        }
    }.start(EnvVars.port)

    logger.info("Checkout Swagger UI at http://localhost:8080/swagger-ui")
} catch (t: Throwable) {
    // Need to manually shut down here because our manager hooks itself up to runtime hooks
    // and starts a background thread. It would keep the JVM alive without javalin running.
    logger.error("Error starting up", t)
    exitProcess(1)
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
