package ai.whylabs.services.whylogs.core

import ai.whylabs.service.invoker.ApiException
import ai.whylabs.services.whylogs.persistent.map.PersistentMap
import ai.whylabs.services.whylogs.persistent.map.SqliteMapWriteLayer
import ai.whylabs.services.whylogs.persistent.queue.PersistentQueue
import ai.whylabs.services.whylogs.persistent.queue.PopSize
import ai.whylabs.services.whylogs.persistent.queue.SqliteQueueWriteLayer
import com.whylogs.core.DatasetProfile
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.actor
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

data class TagsKey(val orgId: String, val datasetId: String, val data: List<Pair<String, String>>)

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
    private val sessionTimeState = currentTime
    private val chronoUnit: ChronoUnit = ChronoUnit.valueOf(period ?: ChronoUnit.HOURS.name)
    private val writer: Writer = SongbirdWriter()

    // TODO keying off of the tags isn't technically correct since it allows you to respecify
    // the org/model in the values, which makes no sense. org/model should be included as a property
    // of the key at some point.
    private val profiles =
        PersistentMap(SqliteMapWriteLayer("profile-entries", TagSerializer(), ProfileEntrySerializer()))

    private val pendingProfiles =
        PersistentQueue(SqliteQueueWriteLayer("pending-profiles", LogRequestSerializer()))

    @Volatile
    private var windowStartTimeState: Instant

    init {
        if (!AllowedChronoUnits.contains(chronoUnit)) {
            throw IllegalArgumentException("Unsupported time units. Please use among: ${AllowedChronoUnits.joinToString { "; " }}")
        }

        logger.info("Using time unit: {}", chronoUnit)
        val nextRun = currentTime.plus(1, chronoUnit).truncatedTo(chronoUnit)
        val initialDelay = nextRun.epochSecond - currentTime.epochSecond
        logger.info("Starting profile manager using time unit: {}", chronoUnit)
        windowStartTimeState = currentTime.truncatedTo(chronoUnit)
        logger.info("Starting with initial window: {}", windowStartTimeState)

        executorService.scheduleWithFixedDelay(
            this::rotate,
            initialDelay,
            Duration.of(1, chronoUnit).seconds,
            TimeUnit.SECONDS
        )

        Runtime.getRuntime().addShutdownHook(Thread(this::stop))
    }

    suspend fun enqueue(request: LogRequest) {
        val requestContainer = LogRequestContainer(
            request = request,
            sessionTime = sessionTimeState,
            windowStartTime = windowStartTimeState
        )
        pendingProfiles.push(listOf(requestContainer))
    }

    // Use a channel to trigger the actual merging because we can leverage the CONFLATED option to avoid processing
    // too many empty requests.
    private val mergeChannel =
        CoroutineScope(
            Executors.newFixedThreadPool(2).asCoroutineDispatcher()
        ).actor<Unit>(capacity = Channel.CONFLATED) {
            for (msg in channel) {
                // Only process once a second. We always process everything that's enqueued so doing it too often
                // results in needless IO since this entire process is asynchronous with the requests anyway.
                delay(1_000)
                handleMergeMessage()
            }
        }

    private suspend fun handleMergeMessage() {
        pendingProfiles.pop(PopSize.All) { pendingEntries ->
            logger.info("Merging ${pendingEntries.size} pending requests into profiles for upload.")

            profiles.reset {
                val storedProfiles = it.toMutableMap()
                logger.debug("Currently ${storedProfiles.size} profiles waiting for upload.")

                pendingEntries.forEach { pendingRequest ->
                    val orgId = EnvVars.orgId
                    val datasetId = pendingRequest.request.datasetId
                    val tags = pendingRequest.request.tags ?: emptyMap()
                    val mapKey =
                        tagsToKey(EnvVars.orgId, pendingRequest.request.datasetId, tags)
                    val existingProfile = storedProfiles.getOrDefault(
                        mapKey, ProfileEntry(
                            profile = DatasetProfile(
                                sessionId,
                                pendingRequest.sessionTime,
                                pendingRequest.windowStartTime,
                                tags + mapOf(DatasetIdTag to datasetId, OrgIdTag to orgId),
                                mapOf()
                            ),
                            orgId = orgId,
                            datasetId = datasetId
                        )
                    )

                    storedProfiles[mapKey] = existingProfile
                }
                logger.debug("Ended with ${storedProfiles.size} profiles waiting for upload.")
                storedProfiles
            }
        }
    }

    fun mergePending() {
        runBlocking {
            mergeChannel.send(Unit)
        }
    }

    private fun stop() = runBlocking {
        logger.debug("Stopping Profile Manager")
        writeOutProfiles()
        logger.info("Shutting down the executor")
        executorService.shutdownNow()
        logger.info("Finished cleaning up resources")
    }

    private fun rotate() = runBlocking {
        logger.info("Rotating logs for the current window: {}", windowStartTimeState)
        writeOutProfiles()
        windowStartTimeState = Instant.now().truncatedTo(chronoUnit)
        logger.info("New window time: {}", windowStartTimeState)
    }

    private suspend fun writeOutProfiles() {
        logger.info("Writing out profiles for window: {}", windowStartTimeState)
        profiles.reset { stagedProfiles ->
            stagedProfiles.filter { profileEntry ->
                logger.info("Writing out profiles for tags: {}", profileEntry.key)
                val profile = profileEntry.value.profile
                val orgId = profileEntry.value.orgId
                val datasetId = profileEntry.value.datasetId

                try {
                    writer.write(profile, orgId, datasetId)
                    false
                } catch (e: ApiException) {
                    logger.error(
                        """
                        API Exception writing to whylabs. Keeping profile ${profileEntry.key} to try later. 
                        Response: ${e.responseBody}
                        Headers: ${e.responseHeaders}""".trimIndent(),
                        e
                    )
                    true
                } catch (e: Exception) {
                    logger.error(
                        "Unexpected exception writing to whylabs. Keeping profile ${profileEntry.key} to try later",
                        e
                    )
                    true
                }
            }
        }
    }
}

