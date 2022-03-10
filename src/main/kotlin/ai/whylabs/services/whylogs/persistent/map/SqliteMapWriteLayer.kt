package ai.whylabs.services.whylogs.persistent.map

import ai.whylabs.services.whylogs.persistent.Serializer
import ai.whylabs.services.whylogs.util.SqliteManager
import org.slf4j.LoggerFactory

/**
 * Implementation of [MapWriteLayer] that uses sqlite as the storage layer.
 */
class SqliteMapWriteLayer<K, V>(
    private val name: String,
    private val keySerializer: Serializer<K>,
    private val valueSerializer: Serializer<V>
) : MapWriteLayer<K, V>, SqliteManager() {
    private val logger = LoggerFactory.getLogger(javaClass)
    override val databaseUrl = "jdbc:sqlite:${System.getProperty("java.io.tmpdir")}/$name-map-v2.sqlite"

    init {
        tx {
            logger.debug("Created sqlite db")
            createStatement().use { it.executeUpdate("CREATE TABLE IF NOT EXISTS items ( key TEXT NOT NULL PRIMARY KEY, value BLOB );") }
        }
    }

    override suspend fun set(key: K, value: V) {
        tx {
            val insertStatement = "INSERT OR REPLACE INTO items (key, value) VALUES (?, ?)"
            prepareStatement(insertStatement).use {
                it.setBytes(1, keySerializer.serialize(key))
                it.setBytes(2, valueSerializer.serialize(value))
                it.executeUpdate()
            }
        }
    }

    override suspend fun get(key: K): V? {
        var item: V? = null
        val query = "SELECT value FROM items WHERE key = ?;"

        query {
            prepareStatement(query).use {
                it.setBytes(1, keySerializer.serialize(key))
                it.executeQuery().use { results ->
                    while (results.next()) {
                        val serializedItem = results.getBytes(1)
                        item = valueSerializer.deserialize(serializedItem)
                    }
                }
            }
        }

        return item
    }

    override suspend fun size(): Int {
        var size: Int? = null
        val query = "SELECT count(1) from items;"
        query {
            prepareStatement(query).use {
                it.executeQuery().use { results ->
                    while (results.next()) {
                        size = results.getInt(1)
                    }
                }
            }
        }

        return size ?: throw IllegalStateException("Couldn't get the size")
    }

    private val deleteStatement = "DELETE FROM items;"
    override suspend fun clear() {
        tx {
            prepareStatement(deleteStatement).use {
                it.executeUpdate()
            }
        }
    }

    override suspend fun remove(key: K) {
        val delete = "DELETE FROM items WHERE key = ?;"

        tx {
            prepareStatement(delete).use {
                it.setBytes(1, keySerializer.serialize(key))
                it.executeUpdate()
            }
        }
    }

    override suspend fun getAll(): Map<K, V> {
        val query = "SELECT key, value FROM items;"
        val items = mutableMapOf<K, V>()

        query {
            prepareStatement(query).use {
                it.executeQuery().use { results ->
                    while (results.next()) {
                        val serializedKey = results.getBytes(1)
                        val serializedValue = results.getBytes(2)
                        items[keySerializer.deserialize(serializedKey)] = valueSerializer.deserialize(serializedValue)
                    }
                }
            }
        }

        return items
    }

    override suspend fun reset(to: Map<K, V>) {
        val insert = "INSERT OR REPLACE INTO items (key, value) VALUES (?, ?)"

        tx {
            prepareStatement(deleteStatement).use {
                it.executeUpdate()
            }

            if (to.isNotEmpty()) {
                prepareStatement(insert).use {
                    to.entries.forEach { (key, value) ->
                        it.setBytes(1, keySerializer.serialize(key))
                        it.setBytes(2, valueSerializer.serialize(value))
                        it.addBatch()
                    }
                    it.executeBatch()
                }
            }
        }
    }
}
