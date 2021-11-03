package ai.whylabs.services.whylogs.persistent.map

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import org.slf4j.LoggerFactory
import java.util.concurrent.Executors

/**
 * A map-like utility that immediately persists all of its contents to disk.
 * @param writer An implementation of [MapWriteLayer] to use for persistence.
 */
class PersistentMap<K, V>(writer: MapWriteLayer<K, V>) : AutoCloseable {
    private val logger = LoggerFactory.getLogger(javaClass)

    private val act =
        mapMessageHandler(
            MapMessageHandlerOptions(
                CoroutineScope(Executors.newFixedThreadPool(2).asCoroutineDispatcher()),
                writer
            )
        )

    /**
     * Get an value from the map.
     * @return null if nothing exists for the key. Else, the associated value.
     */
    suspend fun get(key: K): V? {
        val done = CompletableDeferred<V?>()
        logger.debug("Sending get message and waiting for done signal")
        act.send(PersistentMapMessage.GetMessage(done, key))
        return done.await()
    }

    /**
     * Set a value for the given key.
     * @param block A block that will be passed the current value for the key. The
     * value that this block returns will be used as the new value for the key. Returning
     * null indicates that the entry should be removed.
     */
    suspend fun set(key: K, block: suspend (V?) -> V?) {
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

    /**
     * Reset the content of the map entirely.
     * @param block A block that will be passed the current content of the map (as a [Map]).
     * The map that this block returns will turn into the new content.
     */
    suspend fun reset(block: suspend (Map<K, V>) -> Map<K, V>) {
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

    /**
     * Close the map, making it no longer usable. You should close the map if you intend to create
     * a new one to handle the same data for some reason.
     */
    override fun close() {
        act.close()
    }
}

