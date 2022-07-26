package ai.whylabs.services.whylogs.core

import ai.whylabs.service.invoker.ApiException
import ai.whylabs.services.whylogs.core.config.EnvVars
import ai.whylabs.services.whylogs.core.config.IEnvVars
import ai.whylabs.services.whylogs.core.config.ProfileWritePeriod
import ai.whylabs.services.whylogs.core.config.WriteLayer
import ai.whylabs.services.whylogs.core.writer.Writer
import ai.whylabs.services.whylogs.persistent.QueueBufferedPersistentMap
import ai.whylabs.services.whylogs.persistent.QueueBufferedPersistentMapConfig
import ai.whylabs.services.whylogs.persistent.map.InMemoryMapWriteLayer
import ai.whylabs.services.whylogs.persistent.map.MapMessageHandlerOptions
import ai.whylabs.services.whylogs.persistent.map.PersistentMap
import ai.whylabs.services.whylogs.persistent.map.SqliteMapWriteLayer
import ai.whylabs.services.whylogs.persistent.queue.InMemoryQueueWriteLayer
import ai.whylabs.services.whylogs.persistent.queue.PersistentQueue
import ai.whylabs.services.whylogs.persistent.queue.QueueOptions
import ai.whylabs.services.whylogs.persistent.queue.SqliteQueueWriteLayer
import ai.whylabs.services.whylogs.util.message
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
    currentTime: Instant = Instant.now(), // TODO this needs to be configurable and included on disk somehow for integ tests
    private val envVars: IEnvVars = EnvVars.instance,
    private val writer: Writer = envVars.getProfileWriter(),
    private val orgId: String = envVars.orgId,
    private val sessionId: String = UUID.randomUUID().toString(),
    writeOnStop: Boolean = true,
) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val sessionTimeState = currentTime
    private val chronoUnit: ChronoUnit = envVars.whylogsPeriod

    private val queue = PersistentQueue(
        QueueOptions(
            queueWriteLayer = when (envVars.requestQueueingMode) {
                WriteLayer.IN_MEMORY -> {
                    logger.info("Using InMemoryWriteLayer for request queue.")
                    InMemoryQueueWriteLayer()
                }
                WriteLayer.SQLITE -> {
                    logger.info("Using SqliteQueueWriteLayer for request queue.")
                    SqliteQueueWriteLayer("pending-requests", LogRequestSerializer())
                }
            }
        )
    )

    private val map = PersistentMap(
        MapMessageHandlerOptions(
            when (envVars.profileStorageMode) {
                WriteLayer.IN_MEMORY -> {
                    logger.info("Using InMemoryWriteLayer for profile store.")
                    InMemoryMapWriteLayer()
                }
                WriteLayer.SQLITE -> {
                    logger.info("Using SqliteQueueWriteLayer for profile store.")
                    SqliteMapWriteLayer(
                        "dataset-profiles",
                        ProfileKeySerializer(),
                        ProfileEntrySerializer()
                    )
                }
            }
        )
    )

    internal val config = QueueBufferedPersistentMapConfig(
        queue = queue,
        map = map,
        buffer = envVars.requestQueueingEnabled,
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
            profile.profile.merge(logRequest.request, envVars.ignoredKeys)
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

        if (envVars.profileWritePeriod != ProfileWritePeriod.ON_DEMAND) {
            executorService.scheduleWithFixedDelay(
                this::rotate,
                initialDelay,
                Duration.of(1, envVars.profileWritePeriod.chronoUnit).seconds,
                TimeUnit.SECONDS
            )
        }

        if (writeOnStop) {
            Runtime.getRuntime().addShutdownHook(Thread(this::stop))
        }

        runBlocking {
            initializeProfiles()
        }
    }

    suspend fun handle(request: LogRequest) {
        profiles.merge(
            BufferedLogRequest(
                request = request,
                sessionTime = sessionTimeState,
                windowStartTime = request.timestamp?.let { Instant.ofEpochMilli(it).truncatedTo(chronoUnit) } ?: windowStartTimeState
            )
        )
    }

    suspend fun mergePending() {
        profiles.mergeBuffered(envVars.requestQueueProcessingIncrement)
    }

    private fun stop() = runBlocking {
        logger.debug("Stopping Profile Manager")
        writeOutProfiles()
        logger.info("Shutting down the executor")
        executorService.shutdownNow()
        logger.info("Finished cleaning up resources")
    }

    /**
     * Create empty profiles for each of the dataset ids that we're configured to. This is what
     * keeps the container sending profiles in the absence of receiving actual data, which is a useful
     * property to have to know if things are going wrong.
     */
    private suspend fun initializeProfiles() {
        envVars.emptyProfilesDatasetIds.forEach { datasetId ->
            logger.info("Initializing empty profile for $datasetId")
            handle(
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
        try {
            logger.info("Rotating logs for the current window: {}", windowStartTimeState)
            writeOutProfiles()
            windowStartTimeState = Instant.now().truncatedTo(chronoUnit)
            logger.info("New window time: {}", windowStartTimeState)
            initializeProfiles()
        } catch (t: Throwable) {
            // This function can't throw or it will cancel subsequent runs.
            logger.error("Error when rotating logs for $windowStartTimeState", t)
        }
    }

    internal suspend fun writeOutProfiles(): WriteProfilesResult {
        val profilePaths = mutableListOf<String>()
        var profilesWritten = 0
        profiles.map.process { current ->
            val (key, profileEntry) = current
            logger.info("Writing out profiles for tags: {}", key)

            try {
                writer.write(profileEntry.profile, profileEntry.orgId, profileEntry.datasetId).let { result ->
                    result.uri?.let { profilePaths.add(it) }
                    profilesWritten++
                }

                PersistentMap.ProcessResult.Success()
            } catch (e: ApiException) {
                logger.error(e.message("Keeping profile $key to try later."), e)
                PersistentMap.ProcessResult.RetriableFailure(current, e)
            } catch (e: Throwable) {
                logger.error("Unexpected exception writing profiles. Keeping profile $key to try later", e)
                PersistentMap.ProcessResult.RetriableFailure(current, e)
            }
        }

        return WriteProfilesResult(profilesWritten, profilePaths)
    }
}

data class WriteProfilesResult(
    val profilesWritten: Int,
    val profilePaths: List<String>
)
