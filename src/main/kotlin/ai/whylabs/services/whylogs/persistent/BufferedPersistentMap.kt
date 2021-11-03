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

interface BufferedPersistentMap<BufferedItems, MapKeyType, MapValueType> : AutoCloseable {
    val map: PersistentMap<MapKeyType, MapValueType>
    suspend fun buffer(item: BufferedItems)
    suspend fun mergeBuffered(increment: PopSize, done: CompletableDeferred<Unit>? = null)
}

data class QueueBufferedPersistentMapConfig<BufferedItems, MapKeyType, MapValueType>(
    val queue: PersistentQueue<BufferedItems>,
    val map: PersistentMap<MapKeyType, MapValueType>,

    /**
     * Logic for grouping buffered items into groups, keyed off of the [MapKeyType].
     * Everything that is buffered will eventually make its way into the map. In order
     * to know what key to use in the map, everything in the buffer is grouped according
     * to this function.
     */
    val groupByBlock: (BufferedItems) -> MapKeyType,

    /**
     * The default value used in reduces while merging all of the [BufferedItems] into an
     * existing (or non existing) [MapValueType] for a given key.
     *
     * TODO this is kind of a lame detail that shouldn't really be needed. Think about how to
     * get rid of this thing for the consumer's sake.
     */
    val defaultValue: (MapKeyType) -> MapValueType,

    /**
     * Logic for determining how to transform an item in the buffer into an item that
     * the map stores, and how to reconcile that converted value with the current value
     * in the map if there is one.
     */
    val mergeBlock: (MapValueType, BufferedItems) -> MapValueType,

    /**
     * The amount of time to wait before processing buffered items into the map.
     * This should basically be changed while tuning to achieve some performance target. The
     * default tries to ensure that you don't rapidly process everything in the queue while things
     * are still coming in, leading to lots of little processing requests when it could have all
     * been done in a single go.
     */
    val delay: Long = 1_000,

    val scope: CoroutineScope = CoroutineScope(Executors.newFixedThreadPool(2).asCoroutineDispatcher())
)

private class MergeChannelMessage(val increment: PopSize, val done: CompletableDeferred<Unit>? = null)

/**
 * An implementation of [BufferedPersistentMap] that uses a queue to buffer items of a certain type, Q,
 * before periodically merging them into the main map, transforming them into a V. The types and how
 * to map them are supplied in the configuration.
 */
class QueueBufferedPersistentMap<BufferItems, MayKeyType, MapValueType>(
    private val config: QueueBufferedPersistentMapConfig<BufferItems, MayKeyType, MapValueType>,
) : BufferedPersistentMap<BufferItems, MayKeyType, MapValueType> {
    override val map = config.map
    private val logger = LoggerFactory.getLogger(javaClass)

    private val mergeChannel = config.scope.actor<MergeChannelMessage>(capacity = Channel.CONFLATED) {
        for (msg in channel) {
            // Only process once a second. We always process everything that's enqueued. Doing it too often
            // results in potentially needless IO since this entire process is asynchronous anyway.
            delay(config.delay)
            handleMergeMessage(msg.increment)
            msg.done?.complete(Unit)
        }
    }

    private suspend fun handleMergeMessage(popSize: PopSize = PopSize.All) {
        val block: suspend (List<BufferItems>) -> Unit = { pendingEntries ->
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

    override suspend fun buffer(item: BufferItems) = config.queue.push(listOf(item))

    override suspend fun mergeBuffered(increment: PopSize, done: CompletableDeferred<Unit>?) {
        mergeChannel.send(MergeChannelMessage(increment, done))
    }

    override fun close() {
        config.queue.close()
        config.map.close()
    }
}