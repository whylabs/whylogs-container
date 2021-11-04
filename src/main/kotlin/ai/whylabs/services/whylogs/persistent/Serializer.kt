package ai.whylabs.services.whylogs.persistent

import ai.whylabs.services.whylogs.objectMapper

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

/**
 * Convenience delegator to create serializers that use jackson.
 */
inline fun <reified T> serializer(): Serializer<T> {
    return object : Serializer<T> {
        override fun serialize(t: T): ByteArray {
            return objectMapper.writeValueAsBytes(t)
        }

        override fun deserialize(bytes: ByteArray): T {
            return objectMapper.readValue(bytes, T::class.java)
        }
    }
}
