package ai.whylabs.services.whylogs.persistent.map

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.actor
import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger("queueActor")

internal sealed class PersistentMapMessage<K, V>(val done: CompletableDeferred<V?>) {

    class GetMessage<K, V>(done: CompletableDeferred<V?>, val key: K) : PersistentMapMessage<K, V>(done)

    class SetMessage<K, V>(
        done: CompletableDeferred<V?>,
        val key: K,
        val currentItem: CompletableDeferred<V?>,
        val processingDone: CompletableDeferred<V?>,
    ) : PersistentMapMessage<K, V>(done)

    class ResetMessage<K, V>(
        done: CompletableDeferred<V?>,
        val everything: CompletableDeferred<Map<K, V>>,
        val processingDone: CompletableDeferred<Map<K, V>>,
    ) : PersistentMapMessage<K, V>(done)
}

internal data class MapMessageHandlerOptions<K, V>(
    internal val scope: CoroutineScope,
    internal val writeLayer: MapWriteLayer<K, V>,
)

/**
 * Utility to handle concurrency concerns in the [PersistentMap]. The [PersistentMap] mostly exists
 * to expose a convenient wrapper around the functionality here that doesn't require the caller
 * to create and pass around [CompletableDeferred], which are used under the hood here, along with
 * actors and messages.
 *
 * For the [PersistentMap], there is a single actor that queues all incoming messages so everything is
 * handled sequentially.
 */
internal fun <K, V> mapMessageHandler(options: MapMessageHandlerOptions<K, V>) =
    options.scope.actor<PersistentMapMessage<K, V>>(capacity = Channel.UNLIMITED) {
        for (msg in channel) {
            logger.debug("Handling message ${msg.javaClass}")
            try {
                when (msg) {
                    is PersistentMapMessage.GetMessage<K, V> -> get(options, msg)
                    is PersistentMapMessage.SetMessage<K, V> -> set(options, msg)
                    is PersistentMapMessage.ResetMessage<K, V> -> reset(options, msg)
                }
            } catch (t: Throwable) {
                logger.error("Error handling message ${msg.javaClass}", t)
                msg.done.completeExceptionally(t)
            }
        }

    }

private suspend fun <K, V> get(
    options: MapMessageHandlerOptions<K, V>,
    message: PersistentMapMessage.GetMessage<K, V>
) {
    withTimeout(10_000) {
        logger.debug("Getting item $message.key")
        val item = options.writeLayer.get(message.key)
        message.done.complete(item)
        logger.debug("Done getting item")
    }
}

private suspend fun <K, V> set(
    options: MapMessageHandlerOptions<K, V>,
    message: PersistentMapMessage.SetMessage<K, V>
) {
    withTimeout(10_000) {
        val currentItem = options.writeLayer.get(message.key)

        // Relay the current state of the item
        message.currentItem.complete(currentItem)

        // Wait for them to make up their mind
        val newItem = message.processingDone.await()

        if (newItem == null) {
            options.writeLayer.remove(message.key)
        } else {
            options.writeLayer.set(message.key, newItem)
        }

        message.done.complete(null)
        logger.debug("Done setting item")
    }
}

private suspend fun <K, V> reset(
    options: MapMessageHandlerOptions<K, V>,
    message: PersistentMapMessage.ResetMessage<K, V>
) {
    withTimeout(10_000) {
        val everything = options.writeLayer.getAll()

        // Send everything to the consumer
        message.everything.complete(everything)

        // Get the updated state
        val updatedValues = message.processingDone.await()

        // If its the same ref then there was no change so we're done
        if (everything === updatedValues) {
            message.done.complete(null)
            return@withTimeout
        }

        // Reset the value to this map
        options.writeLayer.reset(updatedValues)

        message.done.complete(null)
        logger.debug("Done resetting item")
    }

}
