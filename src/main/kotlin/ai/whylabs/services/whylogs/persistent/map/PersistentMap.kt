package ai.whylabs.services.whylogs.persistent.map

import ai.whylabs.services.whylogs.util.sentry
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory

/**
 * A map-like utility that immediately persists all of its contents to disk.
 */
class PersistentMap<K, V>(private val options: MapMessageHandlerOptions<K, V>) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val scope = CoroutineScope(Dispatchers.IO)
    private val act = scope.mapMessageHandler(options)

    private inline fun <R> logSentry(name: String, operation: String, block: () -> R): R {
        sentry(name, operation) {
            return block()
        }
    }

    /**
     * Get an value from the map.
     * @return null if nothing exists for the key. Else, the associated value.
     */
    suspend fun get(key: K): V? {
        return withContext(scope.coroutineContext) {
            logSentry("PersistentMap", "get") {
                val done = CompletableDeferred<V?>()
                logger.debug("Sending get message and waiting for done signal")
                act.send(PersistentMapMessage.GetMessage(done, key))
                done.await()
            }
        }
    }

    /**
     * Set a value for the given key.
     * @param block A block that will be passed the current value for the key. The
     * value that this block returns will be used as the new value for the key. Returning
     * null indicates that the entry should be removed.
     */
    suspend fun set(key: K, block: suspend (V?) -> V?) {
        withContext(scope.coroutineContext) {
            logSentry("PersistentMap", "set") {
                val done = CompletableDeferred<V?>()
                val processingDone = CompletableDeferred<V?>()
                val currentItemDeferred = CompletableDeferred<V?>()

                logger.debug("Sending set message and waiting for done signal")
                act.send(PersistentMapMessage.SetMessage(done, key, currentItemDeferred, processingDone))

                val current = currentItemDeferred.await()

                try {
                    processingDone.complete(block(current))
                } catch (t: Throwable) {
                    logger.error("Error while setting value for $key", t)
                    processingDone.completeExceptionally(t)
                }

                done.await()
            }
        }
    }

    /**
     * Reset the content of the map entirely.
     * @param block A block that will be passed the current content of the map (as a [Map]).
     * The map that this block returns will turn into the new content.
     */
    suspend fun reset(block: suspend (Map<K, V>) -> Map<K, V>) {
        withContext(scope.coroutineContext) {
            logSentry("PersistentMap", "reset") {
                val done = CompletableDeferred<V?>()
                val everythingDeferred = CompletableDeferred<Map<K, V>>()
                val processingDone = CompletableDeferred<Map<K, V>>()

                act.send(PersistentMapMessage.ResetMessage(done, everythingDeferred, processingDone))

                logger.debug("Waiting for the current map state")
                val everything = everythingDeferred.await()

                try {
                    processingDone.complete(block(everything))
                } catch (t: Throwable) {
                    logger.error("Error while resetting", t)
                    processingDone.completeExceptionally(t)
                }

                done.await()
            }
        }
    }
}
