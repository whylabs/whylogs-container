package ai.whylabs.services.whylogs.persistent.map

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class PersistentMapTests {

    private var writeLayer: SqliteMapWriteLayer<String, String>? = null
    private lateinit var map: PersistentMap<String, String>

    @BeforeEach
    fun init() {
        writeLayer = SqliteMapWriteLayer("test", StringSerializer(), StringSerializer())
        map = PersistentMap(MapMessageHandlerOptions(writeLayer!!))
        runBlocking { writeLayer?.clear() }
    }

    @AfterEach
    fun after() {
        writeLayer?.close()
    }

    @Test
    fun `map happy path`() = runBlocking<Unit> {
        val key = "foo"

        val foo = map.get(key)
        Assertions.assertEquals(foo, null)

        map.set(key) { currentValue ->
            Assertions.assertEquals(currentValue, null)
            "value"
        }
        val newFoo = map.get(key)
        Assertions.assertEquals(newFoo, "value")
    }

    @Test
    fun `clearing works`() = runBlocking<Unit> {
        val key = "foo"

        // set it
        map.set(key) { currentValue ->
            Assertions.assertEquals(currentValue, null)
            "value"
        }
        val newFoo = map.get(key)
        Assertions.assertEquals(newFoo, "value")

        // clear it
        map.set(key) { currentValue ->
            Assertions.assertEquals(currentValue, "value")
            null
        }
        val clearedFoo = map.get(key)
        Assertions.assertEquals(clearedFoo, null)
    }

    @Test
    fun `removing nonexistent key doesn't do anything`() = runBlocking<Unit> {
        // shouldn't throw
        map.set("key") { currentValue ->
            Assertions.assertEquals(currentValue, null)
            null
        }
    }

    @Test
    fun `reset works`() = runBlocking<Unit> {
        val expected = mapOf(
            "a" to "1",
            "b" to "2",
            "c" to "3",
            "d" to "4",
        )

        expected.entries.forEach { (k, v) ->
            map.set(k) { v }
        }

        map.reset { current ->
            Assertions.assertEquals(expected, current)
            // Set the new value to empty
            emptyMap()
        }

        map.reset { current ->
            // Make sure it worked
            Assertions.assertEquals(emptyMap<String, String>(), current)
            current
        }
    }

    @Test
    fun `process works`() = runBlocking<Unit> {
        val expected = mapOf(
            "a" to "1",
            "b" to "2",
            "c" to "3",
            "d" to "4",
        )

        expected.entries.forEach { (k, v) ->
            map.set(k) { v }
        }

        map.process {
            // mark everything as handled
            PersistentMap.ProcessResult.Success()
        }

        // Use reset just to get a hold of the state for assertions
        map.reset { current ->
            // Make sure it worked
            Assertions.assertEquals(emptyMap<String, String>(), current)
            current
        }
    }

    @Test
    fun `partial process works`() = runBlocking<Unit> {

        map.set("a") { "1" }
        map.set("b") { "2" }
        map.set("c") { "3" }
        map.set("d") { "4" }

        val expected = mapOf(
            "c" to "3",
            "d" to "4",
        )

        map.process {
            // Only handle a and b
            if (it.first == "a" || it.first == "b") {
                PersistentMap.ProcessResult.Success()
            } else {
                PersistentMap.ProcessResult.RetriableFailure(it, IllegalStateException(""))
            }
        }

        // Use reset just to get a hold of the state for assertions
        map.reset { current ->
            // Make sure it worked
            Assertions.assertEquals(expected, current)
            current
        }
    }

    @Test
    fun `partial process works for timeouts`() = runBlocking<Unit> {

        map.set("a") { "1" }
        map.set("b") { "2" }
        map.set("c") { "3" }
        map.set("d") { "4" }

        val timeout = 200L
        val expected = mapOf(
            "c" to "3",
            "d" to "4",
        )

        var iterations = 0
        map.process(timeout) {
            // Only handle a and b
            iterations += 1
            if (it.first == "a" || it.first == "b") {
                PersistentMap.ProcessResult.Success()
            } else {
                // Stall long enough that we get cancelled
                delay(timeout * 2)
                PersistentMap.ProcessResult.PermanentFailure(IllegalStateException())
            }
        }

        // Use reset just to get a hold of the state for assertions
        Assertions.assertEquals(3, iterations)
        map.reset { current ->
            // Make sure it worked
            Assertions.assertEquals(expected, current)
            current
        }
    }
}
