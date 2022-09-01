package ai.whylabs.services.whylogs.core

import ai.whylabs.services.whylogs.core.config.EnvVars
import ai.whylabs.services.whylogs.core.config.IEnvVars
import ai.whylabs.services.whylogs.objectMapper
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.actor
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.util.Date
import java.util.LinkedList

internal data class DebugInfo(
    var env: IEnvVars,
    var storedProfileCount: Int = 0,
    var restLogCalls: Long = 0,
    var profilesWritten: Long = 0,
    var profilesWriteAttempts: Long = 0,
    var profilesWriteFailures: Long = 0,
    var profilesWriteFailureCauses: LinkedList<Pair<Class<*>, Throwable>> = LinkedList(),
    var lastProfileWriteSuccess: Date? = null,
    var lastProfileWriteAttempt: Date? = null,
    var lastProfileWriteFailure: Date? = null,
    var kafkaMessagesHandled: Long = 0,
    var startTime: Date = Date(),
    var uptime: Date? = null,
)

sealed class DebugInfoMessage {
    class RestLogCalledMessage(val n: Int = 1) : DebugInfoMessage()
    class KafkaMessagesHandledMessage(val n: Int = 1) : DebugInfoMessage()
    class ProfileWrittenMessage(val n: Int = 1, val writeTime: Date = Date()) : DebugInfoMessage()
    class ProfileWriteAttemptMessage(val n: Int = 1, val writeTime: Date = Date()) : DebugInfoMessage()
    class ProfileWriteFailuresMessage(val cause: Throwable, val writeTime: Date = Date()) : DebugInfoMessage()
    internal class GetStateMessage(val done: CompletableDeferred<DebugInfo>) : DebugInfoMessage()
    object LogMessage : DebugInfoMessage()
}

data class DebugActorOptions(
    val maxErrors: Int,
    val env: IEnvVars,
    val profileStore: ProfileStore,
)

private val logger = LoggerFactory.getLogger(object {}::class.java.`package`.name)

private fun CoroutineScope.debugActor(options: DebugActorOptions) = actor<DebugInfoMessage>(capacity = 1000) {
    val env = options.env
    val profileStore = options.profileStore
    val info = DebugInfo(env = env)

    suspend fun updateAndLog() {
        val updated = info.copy(
            uptime = Date(Date().time - info.startTime.time),
            storedProfileCount = profileStore.profiles.map.size()
        )

        logger.info(objectMapper.writeValueAsString(updated))
    }

    Runtime.getRuntime().addShutdownHook(
        Thread {
            runBlocking { updateAndLog() }
        }
    )

    for (msg in channel) {
        when (msg) {
            is DebugInfoMessage.LogMessage -> updateAndLog()
            is DebugInfoMessage.RestLogCalledMessage -> info.restLogCalls += msg.n
            is DebugInfoMessage.KafkaMessagesHandledMessage -> info.kafkaMessagesHandled += msg.n
            is DebugInfoMessage.GetStateMessage -> msg.done.complete(info)
            is DebugInfoMessage.ProfileWriteFailuresMessage -> {
                info.profilesWriteFailures += 1
                info.lastProfileWriteFailure = msg.writeTime
                info.profilesWriteFailureCauses.addFirst(Pair(msg.cause.javaClass, msg.cause))
                if (info.profilesWriteFailureCauses.size > options.maxErrors) {
                    info.profilesWriteFailureCauses.removeLast()
                }
            }
            is DebugInfoMessage.ProfileWriteAttemptMessage -> {
                info.profilesWriteAttempts += msg.n
                info.lastProfileWriteAttempt = msg.writeTime
            }

            is DebugInfoMessage.ProfileWrittenMessage -> {
                info.profilesWritten += msg.n
                info.lastProfileWriteSuccess = msg.writeTime
            }
        }
    }
}

class DebugInfoManager internal constructor(
    private val envVars: IEnvVars = EnvVars.instance,
    profileStore: ProfileStore = ProfileStore.instance,
    maxErrors: Int = 10,
) {
    private val debugInfo = CoroutineScope(Dispatchers.IO).debugActor(
        DebugActorOptions(
            env = getEnv(),
            profileStore = profileStore,
            maxErrors = maxErrors,
        )
    )

    suspend fun send(msg: DebugInfoMessage) = debugInfo.send(msg)

    private fun getEnv(): IEnvVars {
        return object : IEnvVars {
            override val writer = envVars.writer
            override val whylabsApiEndpoint = envVars.whylabsApiEndpoint
            override val orgId = envVars.orgId
            override val ignoredKeys = envVars.ignoredKeys
            override val fileSystemWriterRoot = envVars.fileSystemWriterRoot
            override val emptyProfilesDatasetIds = envVars.emptyProfilesDatasetIds
            override val requestQueueingMode = envVars.requestQueueingMode
            override val requestQueueingEnabled = envVars.requestQueueingEnabled
            override val profileStorageMode = envVars.profileStorageMode
            override val requestQueueProcessingIncrement = envVars.requestQueueProcessingIncrement
            override val whylogsPeriod = envVars.whylogsPeriod
            override val s3Prefix = envVars.s3Prefix
            override val s3Bucket = envVars.s3Bucket
            override val port = envVars.port
            override val debug = envVars.debug
            override val kafkaConfig = envVars.kafkaConfig
            override val profileWritePeriod = envVars.profileWritePeriod

            // Omit passwords/secrets. Doing it like this is a bit verbose but it ensures that
            // we won't mistakenly log new secrets we add to the env by mistake automatically.
            override val whylabsApiKey = "--"
            override val expectedApiKey = "--"
        }
    }

    companion object {
        val instance: DebugInfoManager by lazy { DebugInfoManager() }
    }
}
