package ai.whylabs.services.whylogs.core.config

import ai.whylabs.services.whylogs.persistent.queue.PopSize
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.slf4j.LoggerFactory

private val objectMapper = jacksonObjectMapper()

private val log = LoggerFactory.getLogger("EnvParsing")

// TODO expose this and document it when we have a client that hits OOM issues because they have massive requests
// to the container. By default, the requests are cached using sqlite and they are periodically merged into the
// profile storage. If we read everything that we have cached then it might be more memory than we have available.
// The right number for this entirely depends on the expected size of the requests and the memory on the machine.
internal fun parseQueueIncrement(): PopSize {
    val key = EnvVarNames.REQUEST_QUEUE_PROCESSING_INCREMENT.name
    val increment = System.getenv(key) ?: "200"
    if (increment == "ALL") {
        return PopSize.All
    }

    try {
        return PopSize.N(increment.toInt())
    } catch (t: Throwable) {
        throw IllegalStateException("Couldn't parse env key $key", t)
    }
}

internal fun parseEnvList(varName: EnvVarNames): List<String> {
    return try {
        val envVar = varName.getOrDefault()
        objectMapper.readValue(envVar)
    } catch (e: JsonParseException) {
        log.error("Couldn't parse $varName env var. It should be a json list.", e)
        throw e
    }
}

internal fun parseEnvMap(value: String): Map<String, String> {
    return try {
        objectMapper.readValue(value)
    } catch (e: JsonParseException) {
        log.error("Couldn't parse $value. It should be a json map.", e)
        throw e
    }
}
