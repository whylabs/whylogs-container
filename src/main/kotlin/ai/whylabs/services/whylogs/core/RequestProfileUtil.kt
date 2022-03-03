package ai.whylabs.services.whylogs.core

import com.whylogs.core.DatasetProfile
import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger("RequestProfileUtil")

fun DatasetProfile.mergeNested(nestedValue: Map<String, Any>, prefix: String = "") {
    nestedValue.entries.forEach { (feature, value) ->
        when (value) {
            is Map<*, *> -> mergeNested(value as Map<String, Any>, "$prefix$feature.")
            is List<*> -> {
                logger.warn("Dropping value from profile $value")
            }
            else -> {
                logger.debug("Merging $feature into profile with timestamp $dataTimestamp")
                track("$prefix$feature", value)
            }
        }
    }
}

fun DatasetProfile.merge(request: LogRequest) {
    request.single?.let { mergeNested(it) }

    request.multiple?.let {
        it.columns.forEachIndexed { i, featureName ->
            it.data.forEach { data ->
                logger.debug("Merging $featureName into profile with timestamp $dataTimestamp")
                track(featureName, data[i])
            }
        }
    }
}
