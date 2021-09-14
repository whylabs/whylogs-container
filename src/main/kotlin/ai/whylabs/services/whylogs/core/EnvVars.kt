package ai.whylabs.services.whylogs.core

import ai.whylabs.services.whylogs.persistent.queue.PopSize

enum class RequestQueueingMode {
    SQLITE,
    IN_MEMORY
}

class EnvVars {

    companion object {
        val whylabsApiEndpoint = System.getenv("WHYLABS_API_ENDPOINT") ?: "https://api.whylabsapp.com"
        val orgId = System.getenv("ORG_ID") ?: throw IllegalArgumentException("Must supply env var ORG_ID")
        val requestQueueingMode =
            RequestQueueingMode.valueOf(System.getenv("REQUEST_QUEUEING_MODE") ?: RequestQueueingMode.SQLITE.name)
        val requestQueueProcessingIncrement = parseQueueIncrement()
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

// TODO expose this and document it when we have a client that hits OOM issues because they have massive requests
// to the container. By default, the requests are cached using sqlite and they are periodically merged into the
// profile storage. If we read everything that we have cached then it might be more memory than we have available.
// The right number for this entirely depends on the expected size of the requests and the memory on the machine.
private fun parseQueueIncrement(): PopSize {
    val key = "REQUEST_QUEUE_PROCESSING_INCREMENT"
    val increment = System.getenv(key) ?: "ALL"
    if (increment == "ALL") {
        return PopSize.All
    }

    try {
        return PopSize.N(increment.toInt())
    } catch (t: Throwable) {
        throw IllegalStateException("Couldn't parse env key $key", t)
    }
}