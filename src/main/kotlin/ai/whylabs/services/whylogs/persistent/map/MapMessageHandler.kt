package ai.whylabs.services.whylogs.persistent.map

import ai.whylabs.services.whylogs.util.RetryOptions
import ai.whylabs.services.whylogs.util.defaultRetryPolicy
import ai.whylabs.services.whylogs.util.repeatUntilCancelled
import com.github.michaelbull.retry.policy.RetryPolicy
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.actor
import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger("queueActor")
private const val defaultTimeoutMs = 10_000L

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

    class ProcessMessage<K, V>(
        done: CompletableDeferred<V?>,
        val current: SendChannel<Pair<K, V>>,
        val updated: ReceiveChannel<Pair<K, V>?>,
        val timeoutMs: Long? = null,
    ) : PersistentMapMessage<K, V>(done)
}

data class MapMessageHandlerOptions<K, V>(
    val writeLayer: MapWriteLayer<K, V>,
    override val retryPolicy: RetryPolicy<Throwable>? = defaultRetryPolicy
) : RetryOptions

/**
 * Utility to handle concurrency concerns in the [PersistentMap]. The [PersistentMap] mostly exists
 * to expose a convenient wrapper around the functionality here that doesn't require the caller
 * to create and pass around [CompletableDeferred], which are used under the hood here, along with
 * actors and messages.
 *
 * For the [PersistentMap], there is a single actor that queues all incoming messages so everything is
 * handled sequentially.
 */
internal fun <K, V> CoroutineScope.mapMessageHandler(options: MapMessageHandlerOptions<K, V>) =
    actor<PersistentMapMessage<K, V>>(capacity = 100) {
        repeatUntilCancelled(logger) {
            for (msg in channel) {
                logger.debug("Handling message ${msg.javaClass}")
                try {
                    when (msg) {
                        is PersistentMapMessage.GetMessage<K, V> -> get(options, msg)
                        is PersistentMapMessage.SetMessage<K, V> -> set(options, msg)
                        is PersistentMapMessage.ResetMessage<K, V> -> reset(options, msg)
                        is PersistentMapMessage.ProcessMessage<K, V> -> process(options, msg)
                    }
                } catch (t: Throwable) {
                    logger.error("Error handling message ${msg.javaClass}", t)
                    msg.done.completeExceptionally(t)
                }
            }
        }
    }

private suspend fun <K, V> get(
    options: MapMessageHandlerOptions<K, V>,
    message: PersistentMapMessage.GetMessage<K, V>
) {
    withTimeout(defaultTimeoutMs) {
        logger.debug("Getting item $message.key")

        val item = options.retryIfEnabled {
            options.writeLayer.get(message.key)
        }

        message.done.complete(item)
        logger.debug("Done getting item")
    }
}

private suspend fun <K, V> set(
    options: MapMessageHandlerOptions<K, V>,
    message: PersistentMapMessage.SetMessage<K, V>
) {
    withTimeout(defaultTimeoutMs) {
        logger.debug("Looking up current item's value")
        val currentItem = options.retryIfEnabled {
            options.writeLayer.get(message.key)
        }

        logger.debug("Relaying the current state of the item")
        message.currentItem.complete(currentItem)

        logger.debug("Waiting for consumer to process item")
        val newItem = message.processingDone.await()

        if (newItem == null) {
            logger.debug("Deleting the item.")
            options.retryIfEnabled {
                options.writeLayer.remove(message.key)
            }
        } else {
            logger.debug("Updating the item.")
            options.retryIfEnabled {
                options.writeLayer.set(message.key, newItem)
            }
        }

        message.done.complete(null)
        logger.debug("Done setting item")
    }
}

private suspend fun <K, V> reset(
    options: MapMessageHandlerOptions<K, V>,
    message: PersistentMapMessage.ResetMessage<K, V>
) {
    withTimeout(defaultTimeoutMs) {
        logger.debug("Reading everything from the map.")

        val everything = options.retryIfEnabled {
            options.writeLayer.getAll()
        }

        logger.debug("Returning everything to the consumer")
        message.everything.complete(everything)

        // Get the updated state
        logger.debug("Wait for the consumer to finish processing new map state.")
        val updatedValues = message.processingDone.await()

        // If it's the same ref then there was no change and we're done
        if (everything === updatedValues) {
            logger.debug("No changes")
            message.done.complete(null)
            return@withTimeout
        }

        // Reset the value to this map
        logger.debug("Writing the updated value")
        options.retryIfEnabled {
            options.writeLayer.reset(updatedValues)
        }

        message.done.complete(null)
        logger.debug("Done resetting item")
    }
}

private suspend fun <K, V> process(
    options: MapMessageHandlerOptions<K, V>,
    message: PersistentMapMessage.ProcessMessage<K, V>
) {
    logger.debug("Reading everything from the map.")

    val everything = withTimeout(defaultTimeoutMs) {
        options.retryIfEnabled {
            options.writeLayer.getAll()
        }
    }

    logger.debug("Returning everything one by one to the consumer")
    val updatedValues = everything.toMutableMap()
    try {
        everything.entries.forEach { entry ->
            // Send the current value over
            logger.debug("Sending ${entry.key} over for processing")
            message.current.send(entry.toPair())

            // Wait for the new value to pop out
            withTimeout(message.timeoutMs ?: defaultTimeoutMs) {
                logger.debug("Waiting for updated item for ${entry.key}")
                val updatedValue = message.updated.receive()
                if (updatedValue != null) {
                    // update the key if it's not null
                    updatedValues[updatedValue.first] = updatedValue.second
                } else {
                    // otherwise, remove it
                    updatedValues.remove(entry.key)
                }
                logger.debug("Got updated value for ${entry.key}")
            }
        }
    } catch (t: CancellationException) {
        // We just take what we already handled and finish
        logger.error("Error when processing map", t)
    } finally {
        message.current.close()
    }

    if (everything == updatedValues) {
        logger.debug("No changes")
        message.done.complete(null)
        return
    }

    // Reset the value to this map
    logger.debug("Writing the updated value")
    options.retryIfEnabled {
        options.writeLayer.reset(updatedValues)
    }

    message.done.complete(null)
    logger.debug("Done processing item")
}
