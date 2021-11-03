package ai.whylabs.services.whylogs.core

import ai.whylabs.service.invoker.ApiException
import ai.whylabs.services.whylogs.persistent.QueueBufferedPersistentMap
import ai.whylabs.services.whylogs.persistent.QueueBufferedPersistentMapConfig
import ai.whylabs.services.whylogs.persistent.map.PersistentMap
import ai.whylabs.services.whylogs.persistent.map.SqliteMapWriteLayer
import ai.whylabs.services.whylogs.persistent.queue.InMemoryWriteLayer
import ai.whylabs.services.whylogs.persistent.queue.PersistentQueue
import ai.whylabs.services.whylogs.persistent.queue.SqliteQueueWriteLayer
import com.whylogs.core.DatasetProfile
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit


class WhyLogsProfileManager(
    private val executorService: ScheduledExecutorService = Executors.newScheduledThreadPool(1),
    period: String?,
    currentTime: Instant = Instant.now(),
    private val writer: Writer = if (EnvVars.writer == WriterTypes.S3) S3Writer() else SongbirdWriter(),
    private val orgId: String = EnvVars.orgId,
    private val sessionId: String = UUID.randomUUID().toString(),
    writeOnStop: Boolean = true
) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val sessionTimeState = currentTime
    private val chronoUnit: ChronoUnit = ChronoUnit.valueOf(period ?: ChronoUnit.HOURS.name)

    private val queue = PersistentQueue(
        when (EnvVars.requestQueueingMode) {
            RequestQueueingMode.IN_MEMORY -> {
                logger.info("Using InMemoryWriteLayer for request queue.")
                InMemoryWriteLayer()
            }
            RequestQueueingMode.SQLITE -> {
                logger.info("Using SqliteQueueWriteLayer for request queue.")
                SqliteQueueWriteLayer("pending-requests", LogRequestSerializer())
            }
        }
    )
    private val map = PersistentMap(
        SqliteMapWriteLayer(
            "dataset-profiles",
            ProfileKeySerializer(),
            ProfileEntrySerializer()
        )
    )

    internal val config = QueueBufferedPersistentMapConfig(
        queue = queue,
        map = map,
        defaultValue = { profileKey ->
            ProfileEntry(
                profile = DatasetProfile(
                    sessionId,
                    profileKey.sessionTime,
                    profileKey.windowStartTime,
                    profileKey.normalizedTags.toMap() + mapOf(
                        DatasetIdTag to profileKey.datasetId,
                        OrgIdTag to profileKey.orgId
                    ),
                    mapOf()
                ),
                orgId = profileKey.orgId,
                datasetId = profileKey.datasetId
            )
        },
        groupByBlock = { bufferedLogRequest ->
            ProfileKey.fromTags(
                orgId,
                bufferedLogRequest.request.datasetId,
                bufferedLogRequest.request.tags ?: emptyMap(),
                bufferedLogRequest.sessionTime,
                bufferedLogRequest.windowStartTime
            )
        },
        mergeBlock = { profile, logRequest ->
            profile.profile.merge(logRequest.request)
            profile
        }
    )

    internal val profiles = QueueBufferedPersistentMap(config)

    @Volatile
    private var windowStartTimeState: Instant

    init {
        val allowedChronoUnits = setOf(ChronoUnit.HOURS, ChronoUnit.MINUTES, ChronoUnit.DAYS)
        if (!allowedChronoUnits.contains(chronoUnit)) {
            throw IllegalArgumentException("Unsupported time units. Please use among: ${allowedChronoUnits.joinToString { "; " }}")
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

        if (writeOnStop) {
            Runtime.getRuntime().addShutdownHook(Thread(this::stop))
        }

        runBlocking {
            initializeProfiles()
        }
    }

    suspend fun enqueue(request: LogRequest) = profiles.buffer(
        BufferedLogRequest(
            request = request,
            sessionTime = sessionTimeState,
            windowStartTime = windowStartTimeState
        )
    )

    fun mergePending() = runBlocking {
        profiles.mergeBuffered(EnvVars.requestQueueProcessingIncrement)
    }

    private fun stop() = runBlocking {
        logger.debug("Stopping Profile Manager")
        writeOutProfiles()
        logger.info("Shutting down the executor")
        executorService.shutdownNow()
        logger.info("Finished cleaning up resources")
    }

    /**
     * Create emtpy profiles for each of the dataset ids that we're configured to. This is what
     * keeps the container sending profiles in the absence of receiving actual data, which is a useful
     * property to have to know if things are going wrong.
     */
    private suspend fun initializeProfiles() {
        EnvVars.emptyProfilesDatasetIds.forEach { datasetId ->
            logger.info("Initializing empty profile for $datasetId")
            enqueue(
                LogRequest(
                    datasetId = datasetId,
                    tags = null,
                    single = null,
                    multiple = null
                )
            )
        }

        mergePending()
    }

    private fun rotate() = runBlocking {
        logger.info("Rotating logs for the current window: {}", windowStartTimeState)
        writeOutProfiles()
        windowStartTimeState = Instant.now().truncatedTo(chronoUnit)
        logger.info("New window time: {}", windowStartTimeState)
        initializeProfiles()
    }

    private suspend fun writeOutProfiles() {
        logger.info("Writing out profiles for window: {}", windowStartTimeState)

        profiles.map.reset { stagedProfiles ->
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

