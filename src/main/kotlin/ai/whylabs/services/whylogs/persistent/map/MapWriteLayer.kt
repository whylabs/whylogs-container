package ai.whylabs.services.whylogs.persistent.map

/**
 * Acts as the kernel for actually persisting/storing items in a [PersistentMap].
 * Many of these are not exposed through [PersistentMap] but they're required to
 * make certain guarantees in the apis that are exposed.
 *
 * These implementations don't have to try to solve concurrency problems. They'll be
 * used from the [PersistentMap] that deals with concurrency by serializing all access.
 */
interface MapWriteLayer<K, V> {

    /**
     * Set the value V for key K
     */
    suspend fun set(key: K, value: V)

    /**
     * Get the value V for the key K if it exists, or return null.
     */
    suspend fun get(key: K): V?

    /**
     * Get all key/value pairs as a map that are currently stored.
     */
    suspend fun getAll(): Map<K, V>

    /**
     * Wipe out the current state and replace it with [to]
     */
    suspend fun reset(to: Map<K, V>)

    /**
     * Remove the key if it exists. Doesn't do anything if the key doesn't exist.
     */
    suspend fun remove(key: K)

    /**
     * Get the number of entries.
     */
    suspend fun size(): Int

    /**
     * Erase everything.
     */
    suspend fun clear()
}
