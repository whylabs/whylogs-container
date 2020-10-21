package ai.whylabs.services.whylogs.core

import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
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

class WhyLogsProfileManager(
    private val outputPath: String,
    private val executorService: ScheduledExecutorService = Executors.newScheduledThreadPool(1),
    private val chronoUnit: ChronoUnit = ChronoUnit.MINUTES,
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

    private val lock = ReentrantLock()


    init {
        val nextRun = currentTime.plus(1, chronoUnit).truncatedTo(chronoUnit)
        val initialDelay = nextRun.epochSecond - currentTime.epochSecond

        logger.info("Using output path: {}", outputPath)
        logger.info("Starting profile manager using time unit: {}", chronoUnit)
        profiles = ConcurrentHashMap()
        windowStartTime = currentTime.truncatedTo(chronoUnit)
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
            executorService.shutdownNow()
        } finally {
            lock.unlock()
        }
    }

    private fun rotate() {
        lock.lock()
        try {
            logger.info("Log rotation for window: {}", windowStartTime)
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
            val outputFile = Paths.get(outputPath).resolve("profile.${windowStartTime.toEpochMilli()}.${hexSuffix}.bin")
            try {
                Files.newOutputStream(outputFile,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE).use { os ->
                    item.value.toProtobuf().build().writeDelimitedTo(os)
                }
                logger.debug("Wrote output to: {}", outputFile)
            } catch (e: IOException) {
                logger.warn("Failed to write output to path: {}", outputFile, e)
            }
        }
    }
}