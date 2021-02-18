package ai.whylabs.services.whylogs.core

class EnvVars {

    companion object {
        val whylabsApiEndpoint = System.getenv("WHYLABS_API_ENDPOINT") ?: "https://api.whylabsapp.com"
        val orgId = System.getenv("ORG_ID") ?: throw IllegalArgumentException("Must supply env var ORG_ID")
        val whylabsApiKey =
            System.getenv("WHYLABS_API_KEY") ?: throw IllegalArgumentException("Must supply env var WHYLABS_API_KEY")
        val period =
            System.getenv("WHYLOGS_PERIOD") ?: throw IllegalArgumentException("Must supply env var WHYLOGS_PERIOD")

        val expectedApiKey =
            System.getenv("CONTAINER_API_KEY")
                ?: throw IllegalArgumentException("Must supply env var CONTAINER_API_KEY")

        val port = System.getenv("PORT")?.toInt() ?: 8080

        val debug = System.getenv("DEBUG")?.toBoolean() ?: false
    }
}
