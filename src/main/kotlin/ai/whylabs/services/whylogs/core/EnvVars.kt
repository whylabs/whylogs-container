package ai.whylabs.services.whylogs.core

import ai.whylabs.services.whylogs.core.writer.DebugFileSystemWriter
import ai.whylabs.services.whylogs.core.writer.S3Writer
import ai.whylabs.services.whylogs.core.writer.SongbirdWriter
import ai.whylabs.services.whylogs.core.writer.Writer
import ai.whylabs.services.whylogs.persistent.queue.PopSize
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.slf4j.LoggerFactory
import java.time.temporal.ChronoUnit

enum class WriteLayer {
    SQLITE,
    IN_MEMORY
}

enum class WriterTypes {
    S3, WHYLABS, DEBUG_FILE_SYSTEM
}

private const val uploadDestinationEnvVar = "UPLOAD_DESTINATION"
private const val s3BucketEnvVar = "S3_BUCKET"
private const val s3PrefixEnvVar = "S3_PREFIX"
private const val emptyProfilesDatasetIdsEnvVar = "EMPTY_PROFILE_DATASET_IDS"

private val objectMapper = jacksonObjectMapper()

interface IEnvVars {
    val writer: WriterTypes

    fun getProfileWriter(): Writer {
        return when (this.writer) {
            WriterTypes.S3 -> S3Writer(this)
            WriterTypes.WHYLABS -> SongbirdWriter(this)
            WriterTypes.DEBUG_FILE_SYSTEM -> DebugFileSystemWriter(this)
        }
    }

    val whylabsApiEndpoint: String
    val orgId: String

    val emptyProfilesDatasetIds: List<String>
    val requestQueueingMode: WriteLayer
    val profileStorageMode: WriteLayer

    val requestQueueProcessingIncrement: PopSize
    val whylabsApiKey: String

    val fileSystemWriterRoot: String
        get() = "whylogs-profiles"

    /**
     * The period to group profiles into. If this is hourly then you'll end up
     * with hourly profiles. This should probably be set to the same time period
     * as the model that it's uploading to in WhyLabs.
     */
    val whylogsPeriod: ChronoUnit

    /**
     * This controls how often profiles are written out. If this is set to hourly
     * and the [whylogsPeriod] is set to daily then profiles will still be bucked into
     * days, but they'll be written out every hour. By default, this will match the
     * [whylogsPeriod].
     */
    val profileWritePeriod: ProfileWritePeriod
    val expectedApiKey: String

    val s3Prefix: String
    val s3Bucket: String

    val port: Int
    val debug: Boolean
}

enum class ProfileWritePeriod(val chronoUnit: ChronoUnit?) {
    MINUTES(ChronoUnit.MINUTES),
    HOURS(ChronoUnit.HOURS),
    DAYS(ChronoUnit.DAYS),
    ON_DEMAND(null)
}

class EnvVars : IEnvVars {

    private val logger = LoggerFactory.getLogger(EnvVars::class.java)
    override val writer = WriterTypes.valueOf(System.getenv(uploadDestinationEnvVar) ?: WriterTypes.WHYLABS.name)

    override val whylabsApiEndpoint = System.getenv("WHYLABS_API_ENDPOINT") ?: "https://api.whylabsapp.com"
    override val orgId = requireIf(writer == WriterTypes.WHYLABS, "ORG_ID")

    override val emptyProfilesDatasetIds: List<String> = try {
        val envVar = System.getenv(emptyProfilesDatasetIdsEnvVar) ?: "[]"
        objectMapper.readValue(envVar)
    } catch (e: JsonParseException) {
        logger.error("Couldn't parse $emptyProfilesDatasetIdsEnvVar env var. It should be a json list of dataset ids.", e)
        throw e
    }

    override val requestQueueingMode =
        WriteLayer.valueOf(System.getenv("REQUEST_QUEUEING_MODE") ?: WriteLayer.SQLITE.name)
    override val profileStorageMode =
        WriteLayer.valueOf(System.getenv("PROFILE_STORAGE_MODE") ?: WriteLayer.SQLITE.name)
    override val requestQueueProcessingIncrement = parseQueueIncrement()

    override val whylabsApiKey = requireIf(writer == WriterTypes.WHYLABS, "WHYLABS_API_KEY")

    override val whylogsPeriod = ChronoUnit.valueOf(require("WHYLOGS_PERIOD"))
    override val profileWritePeriod = ProfileWritePeriod.valueOf(System.getenv("PROFILE_WRITE_PERIOD") ?: whylogsPeriod.name)
    override val expectedApiKey = require("CONTAINER_API_KEY")

    // Just use a single writer until we get requests otherwise to simplify the error handling logic
    override val s3Prefix = System.getenv(s3PrefixEnvVar) ?: ""
    override val s3Bucket = requireIf(writer == WriterTypes.S3, s3BucketEnvVar)

    override val port = System.getenv("PORT")?.toInt() ?: 8080
    override val debug = System.getenv("DEBUG")?.toBoolean() ?: false
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
    // By default, set it to something large but finite. We have no idea how large the payloads
    // are going to be and if this number is too big then we'll attempt to put all of that into
    // memory at once, which could be bad.
    val requestProcessingIncrement = System.getenv(key) ?: "100"
    if (requestProcessingIncrement == "ALL") {
        return PopSize.All
    }

    try {
        return PopSize.N(requestProcessingIncrement.toInt())
    } catch (t: Throwable) {
        throw IllegalStateException("Couldn't parse env key $key", t)
    }
}
