package ai.whylabs.services.whylogs.persistent

import ai.whylabs.services.whylogs.persistent.map.PersistentMap
import ai.whylabs.services.whylogs.persistent.queue.PersistentQueue
import ai.whylabs.services.whylogs.persistent.queue.PopSize
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.actor
import kotlinx.coroutines.delay
import org.slf4j.LoggerFactory
import java.util.concurrent.Executors

interface BufferedPersistentMap<Q, K, V> : AutoCloseable {
    val map: PersistentMap<K, V>
    suspend fun buffer(item: Q)
    suspend fun mergeBuffered(increment: PopSize, done: CompletableDeferred<Unit>? = null)
}

data class QueueBufferedPersistentMapConfig<Q, K, V>(
    val queue: PersistentQueue<Q>,
    val map: PersistentMap<K, V>,
    val groupByBlock: (Q) -> K,
    val defaultValue: (K) -> V,
    val mergeBlock: (V, Q) -> V,
    val delay: Long = 1_000,
    val scope: CoroutineScope = CoroutineScope(Executors.newFixedThreadPool(2).asCoroutineDispatcher())
)

private class MergeChannelMessage(val increment: PopSize, val done: CompletableDeferred<Unit>? = null)

/**
 * An implementation of [BufferedPersistentMap] that uses a queue to buffer items of a certain type, Q,
 * before periodically merging them into the main map, transforming them into a V. The types and how
 * to map them are supplied in the configuration.
 */
class QueueBufferedPersistentMap<Q, K, V>(
    private val config: QueueBufferedPersistentMapConfig<Q, K, V>,
) : BufferedPersistentMap<Q, K, V> {
    override val map = config.map
    private val logger = LoggerFactory.getLogger(javaClass)

    private val mergeChannel = config.scope.actor<MergeChannelMessage>(capacity = Channel.CONFLATED) {
        for (msg in channel) {
            // Only process once a second. We always process everything that's enqueued so doing it too often
            // results in potentially needless IO since this entire process is asynchronous anyway.
            delay(1_000)
            handleMergeMessage(msg.increment)
            msg.done?.complete(Unit)
        }
    }

    private suspend fun handleMergeMessage(popSize: PopSize = PopSize.All) {
        val block: suspend (List<Q>) -> Unit = { pendingEntries ->
            logger.info("Merging ${pendingEntries.size} pending requests into profiles for upload.")

            // Group all of the entries up by the data that make them unique
            val groupedEntries = pendingEntries.groupBy(config.groupByBlock)

            // For each group, look up the current items that we're storing for the key and update it with each of
            // the pending items that we have buffered.
            groupedEntries.forEach { (key, groupValues) ->
                map.set(key) { currentMapValue ->
                    groupValues.fold(currentMapValue ?: config.defaultValue(key), config.mergeBlock)
                }
            }
        }

        if (popSize == PopSize.All) {
            config.queue.pop(popSize, block)
        } else {
            config.queue.popUntilEmpty(popSize, block)
        }
    }

    override suspend fun buffer(item: Q) = config.queue.push(listOf(item))

    override suspend fun mergeBuffered(increment: PopSize, done: CompletableDeferred<Unit>?) {
        mergeChannel.send(MergeChannelMessage(increment, done))
    }

    override fun close() {
        config.queue.close()
        config.map.close()
    }
}