package ai.whylabs.services.whylogs.core

import ai.whylabs.services.whylogs.persistent.QueueBufferedPersistentMap
import ai.whylabs.services.whylogs.persistent.QueueBufferedPersistentMapConfig
import ai.whylabs.services.whylogs.persistent.Serializer
import ai.whylabs.services.whylogs.persistent.map.PersistentMap
import ai.whylabs.services.whylogs.persistent.map.SqliteMapWriteLayer
import ai.whylabs.services.whylogs.persistent.queue.PersistentQueue
import ai.whylabs.services.whylogs.persistent.queue.PopSize
import ai.whylabs.services.whylogs.persistent.queue.SqliteQueueWriteLayer
import ai.whylabs.services.whylogs.persistent.serializer
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test


class SimpleBufferedPersistentMapTests {
    class StringSerializer : Serializer<String> by serializer()
    class IntSerializer : Serializer<Int> by serializer()

    lateinit var queue: PersistentQueue<String>
    lateinit var config: QueueBufferedPersistentMapConfig<String, String, Int>

    @BeforeEach
    fun init() = runBlocking {
        queue = PersistentQueue(
            SqliteQueueWriteLayer(
                "test-queue",
                StringSerializer()
            )
        )
        config = QueueBufferedPersistentMapConfig(
            queue = queue,
            map = PersistentMap(
                SqliteMapWriteLayer(
                    "test-map",
                    StringSerializer(),
                    IntSerializer()
                ).apply {
                    this.reset(emptyMap())
                }
            ),
            defaultValue = { 0 },
            groupByBlock = { it },
            mergeBlock = { acc, bufferedValue -> acc + bufferedValue.length }
        )

        // Empty the queue out from previous runs
        queue.pop(PopSize.All) { }
    }

    @AfterEach
    fun after() {
    }

    @Test
    fun `buffered items merge into the map correctly`() = runBlocking {
        val bufferedMap = QueueBufferedPersistentMap(config)

        // Add some stuff
        bufferedMap.buffer("first")
        bufferedMap.buffer("second")

        // Merge bufferedMap into the map
        val doneMerging = CompletableDeferred<Unit>()
        bufferedMap.mergeBuffered(PopSize.All, doneMerging)
        doneMerging.await()

        // Make sure buffered items are in the map
        bufferedMap.map.reset { mapContent ->
            Assertions.assertEquals(mapContent, mapOf("first" to 5, "second" to 6))
            mapContent
        }

        // Make sure its not in the queue anymore
        queue.pop(PopSize.All) {
            throw RuntimeException("This shouldn't be called because queuContent should be empty")
        }
        bufferedMap.close()
    }


    @Test
    fun `buffered items are correctly grouped into the right map entries`() = runBlocking {
        val bufferedMap = QueueBufferedPersistentMap(config.copy(
            // Make a's and b's go to the map's "1" key, everything else goes to "2"
            groupByBlock = {
                when (it) {
                    "a" -> "1"
                    "b" -> "1"
                    else -> "2"
                }
            },
            // Just keep track of counts in the main map
            mergeBlock = { acc, _ -> acc + 1 }

        ))

        // Add some stuff
        bufferedMap.buffer("a")
        bufferedMap.buffer("a")
        bufferedMap.buffer("b")
        bufferedMap.buffer("c")

        // Merge bufferedMap into the map
        val doneMerging = CompletableDeferred<Unit>()
        bufferedMap.mergeBuffered(PopSize.All, doneMerging)
        doneMerging.await()

        // Make sure buffered items are in the map
        bufferedMap.map.reset { mapContent ->
            Assertions.assertEquals(mapContent, mapOf("1" to 3, "2" to 1))
            mapContent
        }

        // Make sure its not in the queue anymore
        queue.pop(PopSize.All) {
            throw RuntimeException("This shouldn't be called because queuContent should be empty")
        }
    }
}
