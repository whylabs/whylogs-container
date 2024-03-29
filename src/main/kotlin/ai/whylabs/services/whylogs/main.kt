package ai.whylabs.services.whylogs

import ai.whylabs.services.whylogs.core.WhyLogsController
import ai.whylabs.services.whylogs.core.WhyLogsProfileManager
import ai.whylabs.services.whylogs.core.config.EnvVarNames
import ai.whylabs.services.whylogs.core.config.EnvVars
import ai.whylabs.services.whylogs.core.config.IEnvVars
import ai.whylabs.services.whylogs.kafka.ConsumerController
import com.fasterxml.jackson.core.json.JsonReadFeature
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonMapperBuilder
import io.javalin.Javalin
import io.javalin.apibuilder.ApiBuilder.path
import io.javalin.apibuilder.ApiBuilder.post
import io.javalin.plugin.json.JavalinJackson
import io.javalin.plugin.openapi.OpenApiOptions
import io.javalin.plugin.openapi.OpenApiPlugin
import io.javalin.plugin.openapi.ui.SwaggerOptions
import io.swagger.v3.oas.models.info.Info
import org.slf4j.LoggerFactory
import kotlin.system.exitProcess

private val logger = LoggerFactory.getLogger("ai.whylabs.services.whylogs")
private val mapper = jacksonMapperBuilder()
    .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS) // Allows NaN
    .build().apply {
        // We ignore extra stuff because our `multiple` input can be generated by pandas.to_dict(orient"split), but
        // some extra info will be included in that dict as well, like `index`. Just ignore it rather than modeling it
        // or making people remove it before sending.
        configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    }

fun main() {
    startServer()
}

fun startServer(envVars: IEnvVars = EnvVars.instance): Javalin = try {
    if (envVars.disableAuth) {
        logger.warn("Auth is disabled via the ${EnvVarNames.DISABLE_AUTH} env variable.")
    }

    val profileManager = WhyLogsProfileManager(envVars = envVars)
    if (envVars.kafkaConfig != null) {
        ConsumerController(envVars, profileManager)
    }
    val whylogs = WhyLogsController(envVars, profileManager)

    Javalin.create {
        it.registerPlugin(getConfiguredOpenApiPlugin())
        it.defaultContentType = "application/json"
        it.showJavalinBanner = false
        it.jsonMapper(JavalinJackson(mapper))
    }.apply {
        Runtime.getRuntime().addShutdownHook(Thread { stop() })
        before("pubsubLogs", whylogs::authenticate)
        before("logs", whylogs::authenticate)
        before("writeProfiles", whylogs::authenticate)
        before("logDebugInfo", whylogs::authenticate)

        exception(IllegalArgumentException::class.java) { e, ctx ->
            ctx.json(e.message ?: "Bad Request").status(400)
        }
        routes {
            path("pubsubLogs") { post(whylogs::trackMessage) }
            path("logs") { post(whylogs::track) }
            path("writeProfiles") { post(whylogs::writeProfiles) }
            path("logDebugInfo") { post(whylogs::logDebugInfo) }
            get("health", whylogs::health)
        }
        after("logs", whylogs::after)
        after("pubsubLogs", whylogs::after)
        // TODO make a call to list models to test the api key on startup as a health check
        logger.info("Checkout Swagger UI at http://localhost:${envVars.port}/swagger-ui")
        start(envVars.port)
    }
} catch (t: Throwable) {
    // Need to manually shut down here because our manager hooks itself up to runtime hooks
    // and starts a background thread. It would keep the JVM alive without javalin running.
    logger.error("Error starting up", t)
    exitProcess(1)
}

fun getConfiguredOpenApiPlugin() = OpenApiPlugin(
    OpenApiOptions(
        Info().apply {
            title("whylogs container API")
            version("1.0")
            description("Container that hosts the java version of whylogs behind a REST interface.")
        }
    ).apply {
        path("/swagger-docs") // endpoint for OpenAPI json
        swagger(SwaggerOptions("/swagger-ui")) // endpoint for swagger-ui
    }
)
