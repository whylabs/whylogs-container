package ai.whylabs.services.whylogs.core

import ai.whylabs.services.whylogs.persistent.Serializer
import ai.whylabs.services.whylogs.persistent.serializer
import com.whylogs.core.DatasetProfile
import java.time.Instant

/**
 * Container class that caches the dataset profiles and associated metadata for each request.
 */
data class ProfileEntry(val profile: DatasetProfile, val orgId: String, val datasetId: String)

internal class ProfileEntrySerializer : Serializer<ProfileEntry> by serializer()

internal class LogRequestSerializer : Serializer<BufferedLogRequest> by serializer()

internal class ProfileKeySerializer : Serializer<ProfileKey> by serializer()

data class BufferedLogRequest(
    val request: LogRequest,
    /**
     * This is the time that the server was started. It's mostly useful for debugging
     * profiles and tracing their creation.
     */
    val sessionTime: Instant,
    /**
     * This effectively controls the dataset timestamp.
     */
    val windowStartTime: Instant
)

data class ProfileKey(
    val orgId: String,
    val datasetId: String,
    val normalizedTags: List<Pair<String, String>>,
    val sessionTime: Instant,
    val windowStartTime: Instant

) {

    companion object {
        /**
         * Create a profile key from its components.
         * This mostly converts the tags into a consistent format so that they map to the same profile regardless of
         * the order they come in, so long as they're equal.
         */
        fun fromTags(
            orgId: String,
            datasetId: String,
            tags: Map<String, String>,
            sessionTime: Instant,
            windowStartTime: Instant
        ): ProfileKey {
            val data = tags.keys.sorted().map { tagKey -> Pair(tagKey, tags[tagKey].orEmpty()) }.toList()
            return ProfileKey(orgId, datasetId, data, sessionTime, windowStartTime)
        }
    }
}
