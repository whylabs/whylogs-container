package ai.whylabs.services.whylogs.persistent.map

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory

/**
 * A map-like utility that immediately persists all of its contents to disk.
 */
class PersistentMap<K, V>(options: MapMessageHandlerOptions<K, V>) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val scope = CoroutineScope(Dispatchers.IO)
    private val act = scope.mapMessageHandler(options)

    /**
     * Get an value from the map.
     * @return null if nothing exists for the key. Else, the associated value.
     */
    suspend fun get(key: K): V? {
        return withContext(scope.coroutineContext) {
            val done = CompletableDeferred<V?>()
            logger.debug("Sending get message and waiting for done signal")
            act.send(PersistentMapMessage.GetMessage(done, key))
            done.await()
        }
    }

    /**
     * Get the number of entries.
     */
    suspend fun size(): Int {
        return withContext(scope.coroutineContext) {
            val done = CompletableDeferred<Int>()
            act.send(PersistentMapMessage.SizeMessage(done))
            done.await()
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

    /**
     * Reset the content of the map entirely.
     * @param block A block that will be passed the current content of the map (as a [Map]).
     * The map that this block returns will turn into the new content.
     */
    suspend fun reset(block: suspend (Map<K, V>) -> Map<K, V>) {
        withContext(scope.coroutineContext) {
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

    /**
     * Type that signals the result of each item processed in [process]
     */
    sealed class ProcessResult<V> {
        class Success<V>(val value: V? = null) : ProcessResult<V>()
        class RetriableFailure<V>(val value: V, val exception: Throwable) : ProcessResult<V>()
        class PermanentFailure<V>(val exception: Throwable) : ProcessResult<V>()
    }

    /**
     * Process items in the map one at a time.
     * This is similar to [reset] but instead of handling the content of the entire map it surfaces
     * each item one at a time. The timeout is applied to each individual item rather than the entire
     * map like it does in [reset].
     */
    suspend fun process(timeoutMs: Long? = null, block: suspend (Pair<K, V>) -> ProcessResult<Pair<K, V>>) {
        withContext(scope.coroutineContext) {
            val done = CompletableDeferred<V?>()
            val current = Channel<Pair<K, V>>(capacity = 1)
            val updated = Channel<Pair<K, V>?>(capacity = 1)

            act.send(PersistentMapMessage.ProcessMessage(done, current, updated, timeoutMs))
            logger.debug("Processing map")
            for (item in current) {
                try {
                    logger.debug("Handling current map state for ${item.first}")
                    when (val result = block(item)) {
                        is ProcessResult.Success -> {
                            logger.debug("Successfully handled current map state for ${item.first}")
                            updated.send(result.value)
                        }
                        is ProcessResult.PermanentFailure -> {
                            logger.error("Permanent failure while handling ${item.first}", result.exception)
                            updated.send(null)
                        }
                        is ProcessResult.RetriableFailure -> {
                            logger.error("Failure while handling ${item.first}", result.exception)
                            updated.send(result.value)
                        }
                    }
                } catch (t: Throwable) {
                    logger.error("Error while processing", t)
                    updated.close(t)
                    break
                }
            }

            logger.debug("Done processing map")
            updated.close()
            done.await()
        }
    }
}
