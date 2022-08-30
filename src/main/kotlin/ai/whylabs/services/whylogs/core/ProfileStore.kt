package ai.whylabs.services.whylogs.core

import ai.whylabs.services.whylogs.core.config.EnvVars
import ai.whylabs.services.whylogs.core.config.IEnvVars
import ai.whylabs.services.whylogs.core.config.WriteLayer
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
import com.whylogs.core.DatasetProfile
import org.slf4j.LoggerFactory
import java.util.UUID

class ProfileStore internal constructor(
    private val envVars: IEnvVars = EnvVars.instance,
    private val sessionId: String = UUID.randomUUID().toString(),
) {
    companion object {
        val instance: ProfileStore by lazy { ProfileStore() }
    }

    private val logger = LoggerFactory.getLogger(javaClass)

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
                envVars.orgId,
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
}
