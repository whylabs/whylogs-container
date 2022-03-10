package ai.whylabs.services.whylogs.util

import java.sql.Connection
import java.sql.DriverManager

/**
 * Utility base class for things that have to interface with sqlite.
 */
abstract class SqliteManager : AutoCloseable {
    /**
     * The connection that is used for all of the database operations. Using a single
     * connection is by far the most performant for local sqlite databases but the rest
     * of the application has to be architected in such a way that you don't run into
     * any concurrency issues (which this one is intended to do).
     */
    private var connection: Connection? = null

    /**
     * JDBC string to pass into [DriverManager.getConnection].
     */
    abstract val databaseUrl: String

    init {
        Runtime.getRuntime().addShutdownHook(Thread { connection?.close() })
    }

    override fun close() {
        connection?.close()
    }

    /**
     * Get a hold of [Connection] and initialize the [connection] if its uninitialized
     * or its closed. It should ideally never close but the check is there as a precaution
     * for some weird issue that results in it closing.
     */
    private fun db(block: Connection.() -> Unit) {
        if (connection?.isClosed == true || connection == null) {
            connection = DriverManager.getConnection(databaseUrl)
        }

        connection?.let {
            it.autoCommit = false
            block(it)
        }
    }

    /**
     * Get a hold of a [Connection] to execute queries. If you're going to
     * do any write operations then you should use [tx] instead.
     */
    fun query(block: Connection.() -> Unit) {
        db {
            block(this)
        }
    }

    /**
     * Util function for enabling WAL mode. Make sure to create the database first
     * by executing some SQL statement before this.
     */
    fun enableWAL() {
        DriverManager.getConnection(databaseUrl).use { con ->
            con.createStatement().use { it.executeUpdate("PRAGMA journal_mode=WAL;") }
        }
    }

    fun vacuum() {
        DriverManager.getConnection(databaseUrl).use { con ->
            con.createStatement().use { it.executeUpdate("vacuum;") }
        }
    }

    /**
     * Get a [Connection] within a transaction. This will just commit for you and
     * rollback in a catch block.
     */
    fun tx(block: Connection.() -> Unit) {
        db {
            try {
                block(this)
                commit()
            } catch (t: Throwable) {
                rollback()
                throw t
            }
        }
    }
}
