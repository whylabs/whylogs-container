package ai.whylabs.services.whylogs.core.writer

import ai.whylabs.service.model.SegmentTag
import ai.whylabs.services.whylogs.core.IEnvVars
import ai.whylabs.services.whylogs.core.SegmentTagPrefix
import ai.whylabs.services.whylogs.core.WriterTypes
import com.github.michaelbull.retry.policy.RetryPolicy
import com.github.michaelbull.retry.policy.fullJitterBackoff
import com.github.michaelbull.retry.policy.limitAttempts
import com.github.michaelbull.retry.policy.plus
import com.whylogs.core.DatasetProfile

internal val retryPolicy: RetryPolicy<Throwable> = limitAttempts(3) + fullJitterBackoff(base = 10, max = 5_000)

interface Writer {
    /**
     * @return A string URI where the written profile resides, if one exists.
     */
    suspend fun write(profile: DatasetProfile, orgId: String, datasetId: String): String?

    fun getTagString(tags: List<SegmentTag>): String {
        return if (tags.isEmpty()) "NO_TAGS" else tags.joinToString(",") { "[${it.key}=${it.value}]" }
    }

    /**
     * Get just the user tags that we prefix, without the prefix.
     */
    fun parseTags(profile: DatasetProfile, removePrefixes: Boolean = true): List<SegmentTag> {
        if (removePrefixes) {
            return profile.tags
                .filterKeys { it.startsWith(SegmentTagPrefix) }
                .map { tag ->
                    SegmentTag().apply {
                        key = tag.key.substring(SegmentTagPrefix.length)
                        value = tag.value
                    }
                }
        } else {
            return profile.tags
                .map { tag ->
                    SegmentTag().apply {
                        key = tag.key
                        value = tag.value
                    }
                }
        }
    }

    companion object {
        fun getWriter(envVars: IEnvVars): Writer {
            return when (envVars.writer) {
                WriterTypes.S3 -> S3Writer(envVars)
                WriterTypes.WHYLABS -> SongbirdWriter(envVars)
                WriterTypes.DEBUG_FILE_SYSTEM -> DebugFileSystemWriter(envVars)
            }
        }
    }
}
