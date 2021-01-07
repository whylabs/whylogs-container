package ai.whylabs.services.whylogs.core

import ai.whylabs.songbird.api.LogApi
import ai.whylabs.songbird.invoker.ApiClient
import ai.whylabs.songbird.invoker.Configuration
import ai.whylabs.songbird.invoker.auth.ApiKeyAuth
import org.slf4j.LoggerFactory


class SongbirdClientManager {
    private val logger = LoggerFactory.getLogger(javaClass)

    private val defaultClient: ApiClient = Configuration.getDefaultApiClient()
    val logApi = LogApi(defaultClient)

    init {
        defaultClient.basePath = EnvVars.whylabsApiEndpoint

        // Configure API key authorization: ApiKeyAuth
        val apiKeyAuth = defaultClient.getAuthentication("ApiKeyAuth") as ApiKeyAuth
        logger.warn("Using ${EnvVars.whylabsApiKey}")
        apiKeyAuth.apiKey = EnvVars.whylabsApiKey
    }
}