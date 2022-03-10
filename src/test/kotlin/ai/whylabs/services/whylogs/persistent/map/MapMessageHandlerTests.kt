package ai.whylabs.services.whylogs.persistent.map

import com.github.michaelbull.retry.policy.limitAttempts
import io.mockk.coEvery
import io.mockk.spyk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.lang.IllegalArgumentException

class MapMessageHandlerTests {
    @Test
    fun `putting and getting values works`() = runBlocking {
        val writeLayer = MockMapWriteLayer()
        val map = PersistentMap(MapMessageHandlerOptions(writeLayer))
        map.set("a") { "b" }
        map.set("c") { "d" }

        map.reset {
            Assertions.assertEquals(mapOf("a" to "b", "c" to "d"), it)
            it
        }
    }

    @Test
    fun `write layer recovers`() = runBlocking {
        val writeLayer = spyk(MockMapWriteLayer())

        coEvery { writeLayer.set(any(), any()) } throws (IllegalArgumentException("error")) andThenAnswer { callOriginal() }

        val map = PersistentMap(MapMessageHandlerOptions(writeLayer, null))

        // First one will throw
        try {
            map.set("a") { "b" }
        } catch (t: Throwable) {
        }

        map.set("c") { "d" }

        map.reset {
            Assertions.assertEquals(mapOf("c" to "d"), it)
            it
        }
    }

    @Test
    fun `retries work`() = runBlocking {
        val writeLayer = spyk(MockMapWriteLayer())

        coEvery { writeLayer.set(any(), any()) } throws (IllegalArgumentException("error")) andThenAnswer { callOriginal() }

        val map = PersistentMap(MapMessageHandlerOptions(writeLayer, limitAttempts(2)))

        // First one will throw but internal retries will recover
        map.set("c") { "d" }

        map.reset {
            Assertions.assertEquals(mapOf("c" to "d"), it)
            it
        }
    }
}

private class MockMapWriteLayer : MapWriteLayer<String, String> {
    private var map = mutableMapOf<String, String>()

    override suspend fun set(key: String, value: String) {
        map[key] = value
    }

    override suspend fun get(key: String): String? {
        return map[key]
    }

    override suspend fun getAll(): Map<String, String> {
        return map
    }

    override suspend fun reset(to: Map<String, String>) {
        map = mutableMapOf()
    }

    override suspend fun remove(key: String) {
        map.remove(key)
    }

    override suspend fun size(): Int {
        return map.size
    }

    override suspend fun clear() {
        map.clear()
    }
}
