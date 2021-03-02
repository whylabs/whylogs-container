package ai.whylabs.services.whylogs.core

import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import ai.whylabs.services.whylogs.persistent.map.PersistentMap
import ai.whylabs.services.whylogs.persistent.map.SqliteMapWriteLayer
import com.whylogs.core.DatasetProfile
import kotlinx.coroutines.runBlocking
import org.apache.commons.codec.digest.DigestUtils
import org.slf4j.LoggerFactory


data class TagsKey(val orgId: String, val datasetId: String, val data: List<Pair<String, String>>) {
    fun makeString(): String {
        return "$orgId/$datasetId/${data.joinToString(",") { "${it.first}:${it.second}" }}"
    }
}

private fun tagsToKey(orgId: String, datasetId: String, tags: Map<String, String>): TagsKey {
    val data = tags.keys.sorted().map { tagKey -> Pair(tagKey, tags[tagKey].orEmpty()) }.toList()
    return TagsKey(orgId, datasetId, data)
}

private val AllowedChronoUnits = setOf(ChronoUnit.HOURS, ChronoUnit.MINUTES, ChronoUnit.DAYS)

class WhyLogsProfileManager(
    private val executorService: ScheduledExecutorService = Executors.newScheduledThreadPool(1),
    period: String?,
    currentTime: Instant = Instant.now(),
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    private val sessionId = UUID.randomUUID().toString()
    private val sessionTime = currentTime
    private val chronoUnit: ChronoUnit = ChronoUnit.valueOf(period ?: ChronoUnit.HOURS.name)
    private val writer: Writer = SongbirdWriter()

    // TODO keying off of the tags isn't technically correct since it allows you to respecify
    // the org/model in the values, which makes no sense. org/model should be included as a property
    // of the key at some point.
    private val profiles =
        PersistentMap(SqliteMapWriteLayer("profile-entries", TagSerializer(), ProfileEntrySerializer()))

    @Volatile
    private var windowStartTime: Instant

    init {
        if (!AllowedChronoUnits.contains(chronoUnit)) {
            throw IllegalArgumentException("Unsupported time units. Please use among: ${AllowedChronoUnits.joinToString { "; " }}")
        }

        logger.info("Using time unit: {}", chronoUnit)
        val nextRun = currentTime.plus(1, chronoUnit).truncatedTo(chronoUnit)
        val initialDelay = nextRun.epochSecond - currentTime.epochSecond
        logger.info("Starting profile manager using time unit: {}", chronoUnit)
        windowStartTime = currentTime.truncatedTo(chronoUnit)
        logger.info("Starting with initial window: {}", windowStartTime)

        executorService.scheduleWithFixedDelay(
            this::rotate,
            initialDelay,
            Duration.of(1, chronoUnit).seconds,
            TimeUnit.SECONDS
        )

        Runtime.getRuntime().addShutdownHook(Thread(this::stop))
    }

    suspend fun getProfile(
        tags: Map<String, String>,
        orgId: String,
        datasetId: String,
        block: (ProfileEntry) -> Unit,
    ) {
        val mapKey = tagsToKey(orgId, datasetId, tags)
        profiles.set(mapKey) { current ->
            val datasetTags = mapOf(DatasetIdTag to datasetId, OrgIdTag to orgId)
            val profileEntry = current ?: ProfileEntry(
                profile = DatasetProfile(sessionId,
                    sessionTime,
                    windowStartTime,
                    tags + datasetTags,
                    mapOf()),
                orgId = orgId,
                datasetId = datasetId
            )

            // The request will modify the current as a mutable side effect through the
            // DatasetProfile api. The `current` is effectively a copy serialized on disk so
            // this is safe. Writes happen after each set as well so it's immediately persisted
            // to disk as the new state.
            block(profileEntry)

            profileEntry
        }
    }

    private fun stop() = runBlocking<Unit> {
        logger.debug("Stopping Profile Manager")
        writeOutProfiles()
        logger.info("Shutting down the executor")
        executorService.shutdownNow()
        logger.info("Finished cleaning up resources")
    }

    private fun rotate() = runBlocking<Unit> {
        logger.info("Rotating logs for the current window: {}", windowStartTime)
        writeOutProfiles()
        windowStartTime = Instant.now().truncatedTo(chronoUnit)
        logger.info("New window time: {}", windowStartTime)
    }

    private suspend fun writeOutProfiles() {
        logger.info("Writing out profiles for window: {}", windowStartTime)
        profiles.reset { stagedProfiles ->
            stagedProfiles.filter { profileEntry ->
                logger.info("Writing out profiles for tags: {}", profileEntry.key)
                val hexSuffix = DigestUtils.md5Hex(profileEntry.key.makeString()).substring(0, 10)
                val outputFile = "profile.${windowStartTime.toEpochMilli()}.${hexSuffix}.bin"
                val (profile, orgId, datasetId) = profileEntry.value

                try {
                    writer.write(profile, outputFile, orgId, datasetId)
                    false
                } catch (e: Exception) {
                    logger.error("Failed writing to whylabs. Keeping profile ${profileEntry.key} to try later.", e)
                    true
                }
            }
        }
    }
}

