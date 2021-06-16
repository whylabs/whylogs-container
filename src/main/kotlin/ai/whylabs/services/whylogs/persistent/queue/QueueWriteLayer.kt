package ai.whylabs.services.whylogs.persistent.queue

interface WriteLayer<T> {
    /**
     * Specify whether or not this write layer is capable of handling reads and writes
     * concurrently. The queue handler will parallelize the incoming read/write requests
     * if this can support it. Otherwise, they will be serialized.
     */
    fun concurrentReadWrites(): Boolean
    suspend fun push(t: List<T>)
    suspend fun peek(n: Int): List<T>
    suspend fun pop(n: Int)
    suspend fun size(): Int
    suspend fun clear() {
        val size = this.size()
        if (size > 0) {
            pop(size)
        }
    }
}

