package ai.whylabs.services.whylogs.persistent.queue

interface WriteLayer<T> {
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

