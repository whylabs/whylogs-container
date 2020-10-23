package ai.whylabs.services.whylogs.core

import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import com.whylogs.core.DatasetProfile
import org.apache.commons.codec.digest.DigestUtils
import org.slf4j.LoggerFactory


data class TagsKey(val data: List<Pair<String, String>>) {
    fun makeString(): String {
        return data.joinToString(",") { "${it.first}:${it.second}" }
    }
}

private fun tagsToKey(tags: Map<String, String>): TagsKey {
    val data = tags.keys.sorted().map { tagKey -> Pair(tagKey, tags[tagKey].orEmpty()) }.toList()
    return TagsKey(data)
}

private val AllowedChronoUnits = setOf(ChronoUnit.HOURS, ChronoUnit.MINUTES, ChronoUnit.DAYS)

class WhyLogsProfileManager(
    outputPath: String,
    private val executorService: ScheduledExecutorService = Executors.newScheduledThreadPool(1),
    period: String?,
    awsKmsKeyId: String? = null,
    currentTime: Instant = Instant.now(),
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    private val sessionId = UUID.randomUUID().toString()
    private val sessionTime = currentTime

    @Volatile
    private var isRunning = false

    @Volatile
    private var profiles: ConcurrentMap<TagsKey, DatasetProfile>

    @Volatile
    private var windowStartTime: Instant

    private val writer: Writer

    private val lock = ReentrantLock()
    private val chronoUnit: ChronoUnit = ChronoUnit.valueOf(period ?: ChronoUnit.HOURS.name)

    init {
        if (!AllowedChronoUnits.contains(chronoUnit)) {
            throw IllegalArgumentException("Unsupported time units. Please use among: ${AllowedChronoUnits.joinToString { "; " }}")
        }

        logger.info("Using time unit: {}", chronoUnit)
        val nextRun = currentTime.plus(1, chronoUnit).truncatedTo(chronoUnit)
        val initialDelay = nextRun.epochSecond - currentTime.epochSecond

        writer = if (outputPath.startsWith("s3://")) {
            logger.info("Using S3 writer")

            if (awsKmsKeyId != null) {
                logger.info("Using AWS S3 Server Side Encryption with KMS key: {}", awsKmsKeyId)
            } else {
                logger.info("Using AWS without KMS encryption")
            }
            S3Writer(outputPath, awsKmsKeyId)
        } else {
            LocalWriter(outputPath)
        }

        logger.info("Using output path: {}", outputPath)
        logger.info("Starting profile manager using time unit: {}", chronoUnit)
        profiles = ConcurrentHashMap()
        windowStartTime = currentTime.truncatedTo(chronoUnit)
        logger.info("Starting with initial window: {}", windowStartTime)

        executorService.scheduleWithFixedDelay(
            this::rotate,
            initialDelay,
            Duration.of(1, chronoUnit).seconds,
            TimeUnit.SECONDS)
        isRunning = true

        Runtime.getRuntime().addShutdownHook(Thread(this::stop))
    }

    fun getProfile(tags: Map<String, String>): DatasetProfile {
        if (!isRunning) {
            throw IllegalAccessException("ProfileManager isn't running yet")
        }
        val mapKey = tagsToKey(tags)
        lock.lock()
        try {
            return profiles.computeIfAbsent(mapKey) {
                logger.info("Create new profile for key: {}", it)
                DatasetProfile(sessionId, sessionTime, windowStartTime, tags, mapOf())
            }
        } finally {
            lock.unlock()
        }
    }

    private fun stop() {
        logger.debug("Stopping Profile Manager")
        lock.lock()
        try {
            isRunning = false
            writeOutProfiles()
            logger.info("Shutting down the executor")
            executorService.shutdownNow()
            logger.info("Finished cleaning up resources")
        } finally {
            lock.unlock()
        }
    }

    private fun rotate() {
        lock.lock()
        try {
            logger.info("Rotating logs for the current window: {}", windowStartTime)
            writeOutProfiles()
            profiles = ConcurrentHashMap()
            windowStartTime = Instant.now().truncatedTo(chronoUnit)
            logger.info("New window time: {}", windowStartTime)
        } finally {
            lock.unlock()
        }
    }

    private fun writeOutProfiles() {
        logger.info("Writing out profiles for window: {}", windowStartTime)
        for (item in profiles) {
            val hexSuffix = DigestUtils.md5Hex(item.key.makeString()).substring(0, 10)
            val outputFile = "profile.${windowStartTime.toEpochMilli()}.${hexSuffix}.bin"
            writer.write(item.value, outputFile)
        }
    }
}
