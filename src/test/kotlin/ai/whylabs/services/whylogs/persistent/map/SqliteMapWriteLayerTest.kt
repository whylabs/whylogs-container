package ai.whylabs.services.whylogs.persistent.map

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test


internal class SqliteMapWriteLayerTest {

    private val layer = SqliteMapWriteLayer("SqliteMapWriteLayerTest ", StringSerializer(), StringSerializer())

    @Test
    fun `getting and setting works`() = runBlocking {
        layer.set("Key", "Value")
        val value = layer.get("Key")
        Assertions.assertEquals("Value", value)
    }

    @Test
    fun `reset works`() = runBlocking {
        layer.set("Key", "Value")
        val expected = mapOf("a" to "1", "b" to "2")
        layer.reset(mapOf("a" to "1", "b" to "2"))
        val all = layer.getAll()
        Assertions.assertEquals(expected, all)
        Assertions.assertEquals(2, layer.size())
    }

}