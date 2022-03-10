package ai.whylabs.services.whylogs.persistent.queue

import ai.whylabs.services.whylogs.persistent.Serializer
import com.github.michaelbull.retry.policy.limitAttempts
import io.mockk.coEvery
import io.mockk.every
import io.mockk.spyk
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.nio.charset.Charset

class QueueMessageHandlerTests {

    @Test
    fun `queue recovers from errors in write layer pop`() = runBlocking {
        val mockWriteLayer = spyk(MockQueueWriteLayer<String>())
        val options = QueueOptions(mockWriteLayer, null)

        // Make push throw once
        coEvery { mockWriteLayer.pop(any()) } throws (IllegalArgumentException()) andThenAnswer { callOriginal() }

        PersistentQueue(options).let { queue ->
            queue.push(listOf("a", "b", "c"))

            // will throw in the write layer
            try {
                queue.pop(PopSize.N(2)) { }
            } catch (t: Throwable) {
            }

            queue.push(listOf("d"))

            queue.pop(PopSize.N(4)) { items ->
                Assertions.assertEquals(listOf("a", "b", "c", "d"), items)
            }
        }
    }

    @Test
    fun `push and pop happen in parallel`() = runBlocking {
        withTimeout(20_000) {
            val mockWriteLayer = spyk(MockQueueWriteLayer<String>())
            val options = QueueOptions(mockWriteLayer, limitAttempts(2))

            every { mockWriteLayer.concurrentPushPop } returns (true)
            val queue = PersistentQueue(options)

            val done = CompletableDeferred<Unit>()
            launch {
                queue.pop(PopSize.All) {
                    // wait for the done signal. This would block push if concurrency didn't work
                    done.await()
                }
            }

            // These will execute even though pop is stalled
            queue.push(listOf("a", "b", "c"))
            queue.push(listOf("d"))

            // Which we prove by using peek in the write layer which returns the things that
            // have been saved to the queue. It isn't exposed through the PersistentQueue interface.
            val items = mockWriteLayer.peek(10)
            Assertions.assertEquals(listOf("a", "b", "c", "d"), items)

            // Finally, finish the first pop
            done.complete(Unit)
        }
    }

    @Test
    fun `queue retries failures`() = runBlocking {
        val mockWriteLayer = spyk(MockQueueWriteLayer<String>())
        val options = QueueOptions(mockWriteLayer, limitAttempts(2))

        // Make push throw once
        coEvery { mockWriteLayer.pop(any()) } throws (IllegalArgumentException()) andThenAnswer { callOriginal() }

        PersistentQueue(options).let { queue ->
            queue.push(listOf("a", "b", "c"))

            // will throw in the write layer but the queue will retry over it and the error won't bubble up
            // because it works on the second attempt
            queue.pop(PopSize.N(4)) { items ->
                Assertions.assertEquals(listOf("a", "b", "c"), items)
            }
        }
    }

    @Test
    fun `queue retries failures as specified`() {
        val mockWriteLayer = spyk(MockQueueWriteLayer<String>())
        val options = QueueOptions(mockWriteLayer, limitAttempts(2))

        // Make push throw once
        coEvery { mockWriteLayer.push(any()) } throws (IllegalArgumentException())

        PersistentQueue(options).let { queue ->
            // It will throw more times than the retry count
            Assertions.assertThrows(IllegalArgumentException::class.java) {
                runBlocking {
                    queue.push(listOf("a", "b", "c"))
                }
            }
        }
    }

    @Test
    fun `queue recovers from errors in write layer push`() = runBlocking {
        val mockWriteLayer = spyk(MockQueueWriteLayer<String>())
        val options = QueueOptions(mockWriteLayer, null) // no retries

        // Make push throw once
        coEvery { mockWriteLayer.push(any()) } throws (IllegalArgumentException("mock throw")) andThenAnswer { callOriginal() }

        PersistentQueue(options).let { queue ->
            // will throw in the write layer
            try {
                queue.push(listOf("a", "b", "c"))
            } catch (t: Throwable) {
            }

            queue.push(listOf("d"))

            queue.pop(PopSize.N(4)) { items ->
                Assertions.assertEquals(listOf("d"), items)
            }
        }
    }

    @Test
    fun `push and pop happy path`() = runBlocking {
        PersistentQueue<String>(QueueOptions(MockQueueWriteLayer())).let { queue ->
            queue.push(listOf("a", "b", "c"))

            queue.pop(PopSize.N(2)) { items ->
                Assertions.assertEquals(items, listOf("a", "b"))
            }
        }
    }

    @Test
    fun `popping more than exist returns all`() = runBlocking {
        PersistentQueue<String>(QueueOptions(MockQueueWriteLayer())).let { queue ->
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
            PersistentQueue<String>(QueueOptions(MockQueueWriteLayer())).let { queue ->
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
        PersistentQueue<String>(QueueOptions(MockQueueWriteLayer())).let { queue ->
            queue.push(listOf("a", "b", "c"))

            // There should still be all 3 items left in the queue
            try {
                queue.pop(PopSize.N(20)) { throw Exception("intentional error") }
            } catch (t: Throwable) {
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
        queue = PersistentQueue(QueueOptions(writer))
    }

    @Test
    fun `sqlite happy path`() = runBlocking {
        queue.let {
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
        queue.let {
            it.push(listOf("a", "b", "c"))

            it.pop(PopSize.N(20)) { items ->
                Assertions.assertEquals(items, listOf("a", "b", "c"))
            }
        }
    }

    @Test
    fun `throwing pop doesn't break the queue or drop items`() = runBlocking {
        queue.let {
            it.push(listOf("a", "b", "c"))

            // There should still be all 3 items left in the queue
            try {
                it.pop(PopSize.N(20)) { throw Exception("intentional error") }
            } catch (t: Throwable) {
            }

            it.pop(PopSize.N(3)) { items ->
                Assertions.assertEquals(items, listOf("a", "b", "c"))
            }

            it.pop(PopSize.N(3)) { items ->
                Assertions.assertEquals(items, emptyList<String>())
            }
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
