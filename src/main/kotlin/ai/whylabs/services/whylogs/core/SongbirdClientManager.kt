package ai.whylabs.services.whylogs.core

import ai.whylabs.service.api.LogApi
import ai.whylabs.service.invoker.ApiClient
import ai.whylabs.service.invoker.Configuration
import ai.whylabs.service.invoker.auth.ApiKeyAuth
import org.slf4j.LoggerFactory

private const val ApiKeyIdLength = 10

class SongbirdClientManager {
    private val logger = LoggerFactory.getLogger(javaClass)

    private val defaultClient: ApiClient = Configuration.getDefaultApiClient()
    val logApi = LogApi(defaultClient)

    init {
        defaultClient.basePath = EnvVars.whylabsApiEndpoint

        // Configure API key authorization: ApiKeyAuth
        val apiKeyAuth = defaultClient.getAuthentication("ApiKeyAuth") as ApiKeyAuth
        logger.info("Using WhyLabs API key ID: ${EnvVars.whylabsApiKey.substring(0, ApiKeyIdLength)}")
        apiKeyAuth.apiKey = EnvVars.whylabsApiKey
    }
}