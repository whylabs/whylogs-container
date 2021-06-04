package ai.whylabs.services.whylogs.core

import com.whylogs.core.DatasetProfile
import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger("RequestProfileUtil")

fun DatasetProfile.merge(request: LogRequest) {
    request.single?.let {
        it.forEach { (featureName, value) ->
            logger.debug("Merging $featureName into profile with timestamp $dataTimestamp")
            track(featureName, value)
        }
    }

    request.multiple?.let {
        it.columns.forEachIndexed { i, featureName ->
            it.data.forEach { data ->
                logger.debug("Merging $featureName into profile with timestamp $dataTimestamp")
                track(featureName, data[i])
            }
        }
    }
}


