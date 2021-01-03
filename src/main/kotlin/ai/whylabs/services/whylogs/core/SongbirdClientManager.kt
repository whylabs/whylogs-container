package ai.whylabs.services.whylogs.core

import ai.whylabs.songbird.api.LogApi
import ai.whylabs.songbird.invoker.ApiClient
import ai.whylabs.songbird.invoker.Configuration
import ai.whylabs.songbird.invoker.auth.ApiKeyAuth


class SongbirdClientManager {

    private val defaultClient: ApiClient = Configuration.getDefaultApiClient()
    val logApi = LogApi(defaultClient)

    init {
        defaultClient.basePath = EnvVars.whylabsApiEndpoint

        // Configure API key authorization: ApiKeyAuth
        val apiKeyAuth = defaultClient.getAuthentication("ApiKeyAuth") as ApiKeyAuth
        apiKeyAuth.apiKey = EnvVars.whylabsApiKey
        apiKeyAuth.apiKeyPrefix = "X-API-Key";
    }
}