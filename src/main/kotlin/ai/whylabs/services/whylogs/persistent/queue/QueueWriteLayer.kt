package ai.whylabs.services.whylogs.persistent.queue

interface QueueWriteLayer<T> {
    /**
     * Specify whether this write layer is capable of handling pushing and popping
     * concurrently. The queue handler will parallelize the incoming push/pop requests
     * if this can support it. Otherwise, they will be serialized.
     */
    val concurrentPushPop: Boolean
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
