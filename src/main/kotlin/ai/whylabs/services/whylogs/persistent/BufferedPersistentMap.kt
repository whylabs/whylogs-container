package ai.whylabs.services.whylogs.persistent

import ai.whylabs.services.whylogs.persistent.map.PersistentMap
import ai.whylabs.services.whylogs.persistent.queue.PersistentQueue
import ai.whylabs.services.whylogs.persistent.queue.PopSize
import ai.whylabs.services.whylogs.util.repeatUntilCancelled
import ai.whylabs.services.whylogs.util.sentry
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.actor
import org.slf4j.LoggerFactory

interface BufferedPersistentMap<BufferedItems, MapKeyType, MapValueType> {
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

    val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)
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

    // This channel will be doing a lot of in memory group-by. We'll run it on the default dispatcher instead of the IO one.
    private val mergeChannel = CoroutineScope(Dispatchers.Default).actor<MergeChannelMessage>(capacity = Channel.RENDEZVOUS) {
        repeatUntilCancelled(logger) {
            for (msg in channel) {
                handleMergeMessage(msg.increment)
                msg.done?.complete(Unit)
            }
        }
    }

    private suspend fun handleMergeMessage(popSize: PopSize = PopSize.All) {
        sentry("track", "mergeMessages") { tr ->
            val block: suspend (List<BufferItems>) -> Unit = { pendingEntries ->
                tr.setTag("pendingEntries", pendingEntries.size.toString()) // TODO is this an anti pattern? Should tags have small cardinality?
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

            when (popSize) {
                is PopSize.All -> {
                    config.queue.pop(popSize, block)
                }
                is PopSize.N -> {
                    config.queue.popUntilEmpty(popSize, block)
                }
            }
        }
    }

    override suspend fun buffer(item: BufferItems) = config.queue.push(listOf(item))

    override suspend fun mergeBuffered(increment: PopSize, done: CompletableDeferred<Unit>?) {
        // We don't care about delivering messages if there is already one being handled because the logic
        // of popping items inherently pops until there are none left, triggering this while one is in progress
        // wouldn't matter so we use offer which is just a no-op if the buffer is full, which it will be if
        // anything is happening since the capacity is set to Channel.RENDEZVOUS
        mergeChannel.offer(MergeChannelMessage(increment, done))
    }
}
