package ai.whylabs.services.whylogs.persistent.map

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class SqliteMapQueueWriteLayerTest {

    private var layer: SqliteMapWriteLayer<String, String>? = null

    @BeforeEach
    fun init() {
        layer = SqliteMapWriteLayer("SqliteMapWriteLayerTest ", StringSerializer(), StringSerializer())
    }

    @AfterEach
    fun after() {
        layer?.close()
    }

    @Test
    fun `getting and setting works`() = runBlocking {
        layer?.set("Key", "Value")
        val value = layer?.get("Key")
        Assertions.assertEquals("Value", value)
    }

    @Test
    fun `reset works`() = runBlocking {
        layer?.set("Key", "Value")
        val expected = mapOf("a" to "1", "b" to "2")
        layer?.reset(mapOf("a" to "1", "b" to "2"))
        val all = layer?.getAll()
        Assertions.assertEquals(expected, all)
        Assertions.assertEquals(2, layer?.size())
    }
}
