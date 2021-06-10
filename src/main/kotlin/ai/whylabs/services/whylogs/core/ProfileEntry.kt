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

internal class LogRequestSerializer : Serializer<LogRequestContainer> by serializer()

internal class TagSerializer : Serializer<TagsKey> by serializer()

data class LogRequestContainer(
    val request: LogRequest,
    val sessionTime: Instant,
    val windowStartTime: Instant

)

