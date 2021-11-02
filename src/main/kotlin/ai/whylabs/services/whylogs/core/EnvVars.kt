package ai.whylabs.services.whylogs.core

import ai.whylabs.services.whylogs.persistent.queue.PopSize

enum class RequestQueueingMode {
    SQLITE,
    IN_MEMORY
}

enum class WriterTypes {
    S3, WHYLABS
}

private const val uploadDestinationEnvVar = "UPLOAD_DESTINATION"
private const val s3BucketEnvVar = "S3_BUCKET"
private const val s3PrefixEnvVar = "S3_PREFIX"


class EnvVars {

    companion object {
        val writer = WriterTypes.valueOf(System.getenv(uploadDestinationEnvVar) ?: WriterTypes.WHYLABS.name)

        val whylabsApiEndpoint = System.getenv("WHYLABS_API_ENDPOINT") ?: "https://api.whylabsapp.com"
        val orgId = requireIf(writer == WriterTypes.WHYLABS, "ORG_ID")

        val requestQueueingMode =
            RequestQueueingMode.valueOf(System.getenv("REQUEST_QUEUEING_MODE") ?: RequestQueueingMode.SQLITE.name)
        val requestQueueProcessingIncrement = parseQueueIncrement()

        val whylabsApiKey = requireIf(writer == WriterTypes.WHYLABS, "WHYLABS_API_KEY")
        val period = require("WHYLOGS_PERIOD")
        val expectedApiKey = require("CONTAINER_API_KEY")


        // Just use a single writer until we get requests otherwise to simplify the error handling logic
        val s3Prefix = System.getenv(s3PrefixEnvVar) ?: ""
        val s3Bucket = requireIf(writer == WriterTypes.S3, s3BucketEnvVar)

        val port = System.getenv("PORT")?.toInt() ?: 8080
        val debug = System.getenv("DEBUG")?.toBoolean() ?: false
    }
}

private fun require(envName: String) = requireIf(true, envName)

private fun requireIf(condition: Boolean, envName: String, fallback: String = ""): String {
    val value = System.getenv(envName)

    if (value.isNullOrEmpty() && condition) {
        throw java.lang.IllegalArgumentException("Must supply env var $envName")
    }

    return value ?: fallback
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