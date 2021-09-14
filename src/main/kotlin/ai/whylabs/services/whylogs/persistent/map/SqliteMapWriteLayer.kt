package ai.whylabs.services.whylogs.persistent.map

import ai.whylabs.services.whylogs.persistent.Serializer
import org.slf4j.LoggerFactory
import java.lang.IllegalStateException
import java.sql.Connection
import java.sql.DriverManager

/**
 * Implementation of [MapWriteLayer] that uses sqlite as the storage layer.
 */
class SqliteMapWriteLayer<K, V>(
    private val name: String,
    private val keySerializer: Serializer<K>,
    private val valueSerializer: Serializer<V>
) : MapWriteLayer<K, V> {
    private val logger = LoggerFactory.getLogger(javaClass)

    init {
        val createTable = "CREATE TABLE IF NOT EXISTS items ( key TEXT NOT NULL PRIMARY KEY, value BLOB );"
        db { prepareStatement("vacuum;").execute() }
        db { prepareStatement("PRAGMA journal_mode=WAL;").execute() }
        db {
            logger.debug("Created sqlite db")
            prepareStatement(createTable).execute()
        }
    }

    private fun db(block: Connection.() -> Unit) {
        val url = "jdbc:sqlite:/tmp/$name-map-v2.sqlite"
        DriverManager.getConnection(url).use {
            block(it)
        }
    }

    override suspend fun set(key: K, value: V) {
        val insertStatement = "INSERT OR REPLACE INTO items (key, value) VALUES (?, ?)"

        db {
            prepareStatement(insertStatement).apply {
                val serialized = valueSerializer.serialize(value)
                setBytes(1, keySerializer.serialize(key))
                setBytes(2, serialized)
                executeUpdate()
            }
        }
    }

    override suspend fun get(key: K): V? {
        var item: V? = null
        val query = "SELECT value FROM items WHERE key = ?;"

        db {
            val results = prepareStatement(query).apply {
                setBytes(1, keySerializer.serialize(key))
            }.executeQuery()
            while (results.next()) {
                val serializedItem = results.getBytes(1)
                item = valueSerializer.deserialize(serializedItem)
            }
        }

        return item
    }

    override suspend fun size(): Int {
        var size: Int? = null
        val query = "SELECT count(1) from items;"
        db {
            val results = prepareStatement(query).executeQuery()

            while (results.next()) {
                size = results.getInt(1)
            }
        }

        return size ?: throw IllegalStateException("Couldn't get the size")
    }

    private val deleteStatement = "DELETE FROM items;"
    override suspend fun clear() {
        db {
            prepareStatement(deleteStatement).executeUpdate()
        }
    }

    override suspend fun remove(key: K) {
        val delete = "DELETE FROM items WHERE key = ?;"

        db {
            prepareStatement(delete).apply {
                setBytes(1, keySerializer.serialize(key))
                executeUpdate()
            }
        }
    }

    override suspend fun getAll(): Map<K, V> {
        val query = "SELECT key, value FROM items;"
        val items = mutableMapOf<K, V>()

        db {
            val results = prepareStatement(query).executeQuery()
            while (results.next()) {
                val serializedKey = results.getBytes(1)
                val serializedValue = results.getBytes(2)
                items[keySerializer.deserialize(serializedKey)] = valueSerializer.deserialize(serializedValue)
            }
        }

        return items
    }

    override suspend fun reset(to: Map<K, V>) {
        val insert = "INSERT OR REPLACE INTO items (key, value) VALUES (?, ?)"

        db {
            autoCommit = false
            try {
                prepareStatement(deleteStatement).executeUpdate()

                prepareStatement(insert).apply {
                    to.entries.forEach { (key, value) ->
                        setBytes(1, keySerializer.serialize(key))
                        setBytes(2, valueSerializer.serialize(value))
                        addBatch()
                    }
                }.executeBatch()
                commit()
            } catch (t: Throwable) {
                rollback()
                throw t
            }
        }
        db { prepareStatement("vacuum;").execute() }
    }

}
