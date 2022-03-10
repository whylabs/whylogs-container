package ai.whylabs.services.whylogs.persistent.queue

import ai.whylabs.services.whylogs.persistent.Serializer
import ai.whylabs.services.whylogs.util.SqliteManager
import org.slf4j.LoggerFactory

class SqliteQueueWriteLayer<T>(private val name: String, private val serializer: Serializer<T>) : QueueWriteLayer<T>, SqliteManager() {
    private val logger = LoggerFactory.getLogger(javaClass)
    override val databaseUrl: String = "jdbc:sqlite:${System.getProperty("java.io.tmpdir")}/$name-queue-v2.sqlite"

    init {
        tx {
            logger.debug("Created sqlite db")
            createStatement().use { it.executeUpdate("CREATE TABLE IF NOT EXISTS items ( value BLOB );") }
        }

        enableWAL()
    }

    override suspend fun push(t: List<T>) {
        val values = t.map { serializer.serialize(it) }
        val insert = "INSERT INTO items (value) VALUES (?)"

        tx {
            prepareStatement(insert).use { statement ->
                values.forEach { value ->
                    statement.setBytes(1, value)
                    statement.addBatch()
                }
                statement.executeBatch()
            }
        }
    }

    override suspend fun peek(n: Int): List<T> {
        val items = mutableListOf<T>()
        val query = "SELECT value FROM items ORDER BY ROWID ASC LIMIT $n;"

        query {
            prepareStatement(query).use {
                it.executeQuery().use { results ->
                    while (results.next()) {
                        val serializedItem = results.getBytes(1)
                        val item = serializer.deserialize(serializedItem)
                        items.add(item)
                    }
                }
            }
        }

        return items
    }

    override suspend fun pop(n: Int) {
        // TODO this query is nicer and probably performs better but it requires sqlite to be built
        // with  SQLITE_ENABLE_UPDATE_DELETE_LIMIT flag.
        // val query = "DELETE FROM items ORDER BY ROWID ASC LIMIT $n;"
        val query = "DELETE FROM items WHERE ROWID IN ( SELECT ROWID FROM items ORDER BY ROWID ASC LIMIT $n);"
        tx {
            createStatement().use { it.executeUpdate(query) }
        }
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

    override val concurrentPushPop = true
}
