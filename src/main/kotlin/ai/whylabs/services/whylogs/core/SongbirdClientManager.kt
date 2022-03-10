package ai.whylabs.services.whylogs.core

import ai.whylabs.service.api.LogApi
import ai.whylabs.service.invoker.ApiClient
import ai.whylabs.service.invoker.Configuration
import ai.whylabs.service.invoker.auth.ApiKeyAuth
import ai.whylabs.services.whylogs.core.config.EnvVars
import ai.whylabs.services.whylogs.core.config.IEnvVars
import org.slf4j.LoggerFactory

private const val ApiKeyIdLength = 10

class SongbirdClientManager(envVars: IEnvVars = EnvVars.instance) {
    private val logger = LoggerFactory.getLogger(javaClass)

    private val defaultClient: ApiClient = Configuration.getDefaultApiClient()
    val logApi = LogApi(defaultClient)

    init {
        // TODO disable this path when whylabs isn't configured
        defaultClient.basePath = envVars.whylabsApiEndpoint

        // Configure API key authorization: ApiKeyAuth
        val apiKeyAuth = defaultClient.getAuthentication("ApiKeyAuth") as ApiKeyAuth
        logger.info("Using WhyLabs API key ID: ${envVars.whylabsApiKey.substring(0, ApiKeyIdLength)}")
        apiKeyAuth.apiKey = envVars.whylabsApiKey
    }
}
