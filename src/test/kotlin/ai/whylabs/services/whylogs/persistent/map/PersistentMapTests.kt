package ai.whylabs.services.whylogs.persistent.map

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class PersistentMapTests {

    private val writeLayer = SqliteMapWriteLayer("test", StringSerializer(), StringSerializer())
    private lateinit var map: PersistentMap<String, String>

    @BeforeEach
    fun init() {
        map = PersistentMap(writeLayer)
        runBlocking { writeLayer.clear() }
    }

    @AfterEach
    fun after() {
        map.close()
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
}
