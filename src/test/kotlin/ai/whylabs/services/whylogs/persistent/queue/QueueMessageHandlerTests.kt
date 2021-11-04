package ai.whylabs.services.whylogs.persistent.queue

import ai.whylabs.services.whylogs.persistent.Serializer
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.nio.charset.Charset

class QueueMessageHandlerTests {

    @Test
    fun `push and pop happy path`() = runBlocking {
        PersistentQueue<String>(MockQueueWriteLayer()).use { queue ->
            queue.push(listOf("a", "b", "c"))

            queue.pop(PopSize.N(2)) { items ->
                Assertions.assertEquals(items, listOf("a", "b"))
            }
        }
    }

    @Test
    fun `popping more than exist returns all`() = runBlocking {
        PersistentQueue<String>(MockQueueWriteLayer()).use { queue ->
            queue.push(listOf("a", "b", "c"))

            queue.pop(PopSize.N(20)) { items ->
                Assertions.assertEquals(items, listOf("a", "b", "c"))
            }
        }
    }

    @Test
    fun `popping doesn't block pushing`() = runBlocking {
        // If pop does block then this test will fail with a timeout
        withTimeout(5_000) {
            PersistentQueue<String>(MockQueueWriteLayer()).use { queue ->
                queue.push(listOf("a", "b", "c"))

                val popDone = CompletableDeferred<Unit>()
                val popStarted = CompletableDeferred<Unit>()
                async {
                    queue.pop(PopSize.N(20)) {
                        popStarted.complete(Unit)
                        popDone.await()
                    }
                }

                popStarted.await()
                queue.push(listOf("a", "b", "c"))
                popDone.complete(Unit)
            }
        }
    }

    @Test
    fun `throwing in pop doesn't break the queue or drop items`() = runBlocking {
        PersistentQueue<String>(MockQueueWriteLayer()).use { queue ->
            queue.push(listOf("a", "b", "c"))

            // There should still be all 3 items left in the queue
            try {
                queue.pop(PopSize.N(20)) { throw Exception("oops") }
            } catch (t: Throwable) {
                println("Successfully failed")
            }

            queue.pop(PopSize.N(3)) { items ->
                Assertions.assertEquals(items, listOf("a", "b", "c"))
            }

            queue.pop(PopSize.N(3)) { items ->
                Assertions.assertEquals(items, emptyList<String>())
            }
        }
    }
}

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PersistentSqliteQueueTests {

    private val writer = SqliteQueueWriteLayer("name", StringSerializer())
    private lateinit var queue: PersistentQueue<String>

    @BeforeEach
    fun init() = runBlocking {
        writer.clear()
        queue = PersistentQueue(writer)
    }

    @Test
    fun `sqlite happy path`() = runBlocking {
        queue.use {
            it.push(listOf("a", "b", "c"))

            it.pop(PopSize.N(2)) { items ->
                Assertions.assertEquals(items, listOf("a", "b"))
            }

            it.pop(PopSize.N(2)) { items ->
                Assertions.assertEquals(items, listOf("c"))
            }
        }
    }

    @Test
    fun `popping more than exist returns all`() = runBlocking {
        queue.use {
            it.push(listOf("a", "b", "c"))

            it.pop(PopSize.N(20)) { items ->
                Assertions.assertEquals(items, listOf("a", "b", "c"))
            }
        }
    }

    @Test
    fun `throwing pop doesn't break the queue or drop items`() = runBlocking {
        queue.use {
            it.push(listOf("a", "b", "c"))

            // There should still be all 3 items left in the queue
            try {
                it.pop(PopSize.N(20)) { throw Exception("oops") }
            } catch (t: Throwable) {
            }

            println("a")
            it.pop(PopSize.N(3)) { items ->
                Assertions.assertEquals(items, listOf("a", "b", "c"))
            }

            println("b")
            it.pop(PopSize.N(3)) { items ->
                Assertions.assertEquals(items, emptyList<String>())
            }
            println("c")
        }
    }
}

class StringSerializer : Serializer<String> {
    override fun deserialize(bytes: ByteArray): String {
        return bytes.toString(Charset.forName("utf-8"))
    }

    override fun serialize(t: String): ByteArray {
        return t.toByteArray(Charset.forName("utf-8"))
    }
}
