package ai.whylabs.services.whylogs.core

import com.whylogs.core.DatasetProfile
import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger("RequestProfileUtil")

fun DatasetProfile.mergeNested(nestedValue: Map<String, Any>, ignored: Set<String>, prefix: String = "") {
    nestedValue.entries.forEach { (feature, value) ->
        val key = "$prefix$feature"
        when (value) {
            is Map<*, *> -> mergeNested(value as Map<String, Any>, ignored, "$key.")
            is List<*> -> {
                logger.warn("Dropping value from profile $value because it's a list")
            }
            else -> {
                logger.debug("Merging $feature into profile with timestamp $dataTimestamp")
                if (!ignored.contains(key)) {
                    logger.debug("Ignoring $key according to container configuration")
                    track(key, value)
                }
            }
        }
    }
}

fun DatasetProfile.merge(request: LogRequest, ignored: Set<String> = emptySet()) {
    request.single?.let { mergeNested(it, ignored) }

    request.multiple?.let {
        it.columns.forEachIndexed { i, featureName ->
            it.data.forEach { data ->
                logger.debug("Merging $featureName into profile with timestamp $dataTimestamp")
                if (!ignored.contains(featureName)) {
                    track(featureName, data[i])
                }
            }
        }
    }
}
