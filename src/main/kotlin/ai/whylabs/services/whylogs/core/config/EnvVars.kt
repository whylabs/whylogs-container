package ai.whylabs.services.whylogs.core.config

import ai.whylabs.services.whylogs.core.writer.DebugFileSystemWriter
import ai.whylabs.services.whylogs.core.writer.S3Writer
import ai.whylabs.services.whylogs.core.writer.WhyLabsWriter
import ai.whylabs.services.whylogs.core.writer.Writer
import ai.whylabs.services.whylogs.persistent.queue.PopSize
import com.fasterxml.jackson.annotation.JsonIgnore
import java.time.Duration
import java.time.temporal.ChronoUnit

enum class WriteLayer {
    SQLITE,
    IN_MEMORY
}

enum class WriterTypes {
    S3, WHYLABS, DEBUG_FILE_SYSTEM
}

enum class WhylogsPeriod(val chronoUnit: ChronoUnit) {
    HOURS(ChronoUnit.HOURS),
    DAYS(ChronoUnit.DAYS),
}

enum class ProfileWritePeriod(val chronoUnit: ChronoUnit?) {
    FIVE_MINUTES(null),
    TEN_MINUTES(null),
    THIRTY_MINUTES(null),
    HOURS(ChronoUnit.HOURS),
    DAYS(ChronoUnit.DAYS),

    /**
     * Don't automatically upload profiles. Uploading only happens when triggered
     * via the REST interface.
     */
    ON_DEMAND(null);

    fun asDuration(): Duration? {
        return when (this) {
            FIVE_MINUTES -> return Duration.of(5, ChronoUnit.MINUTES)
            TEN_MINUTES -> return Duration.of(10, ChronoUnit.MINUTES)
            THIRTY_MINUTES -> return Duration.of(30, ChronoUnit.MINUTES)
            ON_DEMAND -> return null

            HOURS -> return Duration.of(1, ChronoUnit.HOURS)
            DAYS -> return Duration.of(1, ChronoUnit.DAYS)
        }
    }
}

interface IEnvVars {
    val writer: WriterTypes
    val whylabsApiEndpoint: String
    val orgId: String
    val ignoredKeys: Set<String>

    val fileSystemWriterRoot: String

    val emptyProfilesDatasetIds: List<String>
    val disableAuth: Boolean
    val requestQueueingMode: WriteLayer
    val requestQueueingEnabled: Boolean

    val profileStorageMode: WriteLayer
    val requestQueueProcessingIncrement: PopSize
    val whylabsApiKey: String

    /**
     * The period to group profiles into. If this is hourly then you'll end up
     * with hourly profiles. This should probably be set to the same time period
     * as the model that it's uploading to in WhyLabs.
     */
    val whylogsPeriod: ChronoUnit

    val expectedApiKey: String

    val s3Prefix: String
    val s3Bucket: String
    val port: Int
    val debug: Boolean
    val kafkaConfig: KafkaConfig?

    /**
     * This controls how often profiles are written out. If this is set to hourly
     * and the [whylogsPeriod] is set to daily then profiles will still be bucked into
     * days, but they'll be written out every hour. By default, this will match the
     * [whylogsPeriod].
     */
    val profileWritePeriod: ProfileWritePeriod

    @JsonIgnore
    fun getProfileWriter(): Writer {
        return when (this.writer) {
            WriterTypes.S3 -> S3Writer(this)
            WriterTypes.WHYLABS -> WhyLabsWriter(this)
            WriterTypes.DEBUG_FILE_SYSTEM -> DebugFileSystemWriter(this)
        }
    }
}

class EnvVars private constructor() : IEnvVars {
    override val writer = WriterTypes.valueOf(EnvVarNames.UPLOAD_DESTINATION.getOrDefault())

    override val whylabsApiEndpoint = EnvVarNames.WHYLABS_API_ENDPOINT.getOrDefault()
    override val orgId = EnvVarNames.ORG_ID.requireIf(writer == WriterTypes.WHYLABS)

    override val kafkaConfig = KafkaConfig.parse(this)

    override val emptyProfilesDatasetIds: List<String> = parseEnvList(EnvVarNames.EMPTY_PROFILE_DATASET_IDS)
    override val disableAuth: Boolean = EnvVarNames.DISABLE_AUTH.getOrDefault().toBoolean()
    override val ignoredKeys: Set<String> = parseEnvList(EnvVarNames.IGNORED_KEYS).toSet()

    override val requestQueueingMode = WriteLayer.valueOf(EnvVarNames.REQUEST_QUEUEING_MODE.getOrDefault())
    override val profileStorageMode = WriteLayer.valueOf(EnvVarNames.PROFILE_STORAGE_MODE.getOrDefault())
    override val requestQueueingEnabled = EnvVarNames.REQUEST_QUEUEING_ENABLED.getOrDefault().toBoolean()

    override val fileSystemWriterRoot = EnvVarNames.FILE_SYSTEM_WRITER_ROOT.getOrDefault()

    override val requestQueueProcessingIncrement = parseQueueIncrement()

    override val whylabsApiKey = EnvVarNames.WHYLABS_API_KEY.requireIf(writer == WriterTypes.WHYLABS)
    override val expectedApiKey = EnvVarNames.CONTAINER_API_KEY.require().trim()

    // Just use a single writer until we get requests otherwise to simplify the error handling logic
    override val s3Prefix = EnvVarNames.S3_PREFIX.requireIf(writer == WriterTypes.S3)
    override val s3Bucket = EnvVarNames.S3_BUCKET.requireIf(writer == WriterTypes.S3)

    override val whylogsPeriod = validateWhylogsPeriod()
    override val profileWritePeriod = ProfileWritePeriod.valueOf(EnvVarNames.PROFILE_WRITE_PERIOD.get() ?: whylogsPeriod.name)

    override val port = EnvVarNames.PORT.getOrDefault().toInt()
    override val debug = false

    companion object {
        val instance: IEnvVars by lazy { EnvVars() }
    }
}

private fun validateWhylogsPeriod(): ChronoUnit {
    val chrono = ChronoUnit.valueOf(EnvVarNames.WHYLOGS_PERIOD.require())

    if (chrono !== ChronoUnit.DAYS && chrono !== ChronoUnit.HOURS) {
        throw IllegalArgumentException("whylogs period must be either DAYS or HOURS")
    }

    return chrono
}
