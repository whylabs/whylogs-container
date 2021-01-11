package ai.whylabs.services.whylogs.persistent

// This is split off in its own package because there may be cause to add data structures
// aside from `map`. This would be used for those new ones as well.

/**
 * Serializer interface used by the persistent data structures to write
 * files to disk.
 */
interface Serializer<T> {
    fun serialize(t: T): ByteArray
    fun deserialize(bytes: ByteArray): T
}