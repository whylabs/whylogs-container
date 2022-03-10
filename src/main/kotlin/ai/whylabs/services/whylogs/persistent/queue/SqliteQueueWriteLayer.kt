package ai.whylabs.services.whylogs.persistent.queue

import ai.whylabs.services.whylogs.persistent.Serializer
import org.slf4j.LoggerFactory
import java.lang.IllegalStateException
import java.sql.Connection
import java.sql.DriverManager

class SqliteQueueWriteLayer<T>(private val name: String, private val serializer: Serializer<T>) : QueueWriteLayer<T> {
    private val logger = LoggerFactory.getLogger(javaClass)

    init {
        val createTable = "CREATE TABLE IF NOT EXISTS items ( value BLOB );"
        db { prepareStatement("vacuum;").execute() }
        db { prepareStatement("PRAGMA journal_mode=WAL;").execute() }
        db {
            logger.debug("Created sqlite db")

            prepareStatement(createTable).execute()
        }
    }

    private fun db(block: Connection.() -> Unit) {
        val url = "jdbc:sqlite:/tmp/$name-queue-v2.sqlite"
        DriverManager.getConnection(url).use {
            block(it)
        }
    }

    override suspend fun push(t: List<T>) {
        val values = t.map { serializer.serialize(it) }
        val insert = "INSERT INTO items (value) VALUES (?)"

        db {
            values.forEach { value ->
                prepareStatement(insert).apply {
                    setBytes(1, value)
                    executeUpdate()
                }
            }
        }
    }

    override suspend fun peek(n: Int): List<T> {
        val items = mutableListOf<T>()
        val query = "SELECT value FROM items ORDER BY ROWID ASC LIMIT $n;"

        db {
            val results = prepareStatement(query).executeQuery()
            while (results.next()) {
                val serializedItem = results.getBytes(1)
                val item = serializer.deserialize(serializedItem)
                items.add(item)
            }
        }

        return items
    }

    override suspend fun pop(n: Int) {
        // TODO this query is nicer and probably performs better but it requires sqlite to be built
        // with  SQLITE_ENABLE_UPDATE_DELETE_LIMIT flag.
        // val query = "DELETE FROM items ORDER BY ROWID ASC LIMIT $n;"
        val query = "DELETE FROM items WHERE ROWID IN ( SELECT ROWID FROM items ORDER BY ROWID ASC LIMIT $n);"
        db {
            prepareStatement(query).executeUpdate()
        }
        // Vacuuming after deleting items. Without this, the size of the sqlite database file would never shrink.
        // This is an OK spot to do this but it might end up impacting performance a bit. It would probably be better
        // to just do it on some interval.
        db { prepareStatement("vacuum;").execute() }
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

    override val concurrentReadWrites = true
}
