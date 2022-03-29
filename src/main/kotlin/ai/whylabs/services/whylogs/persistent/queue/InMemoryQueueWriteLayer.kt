package ai.whylabs.services.whylogs.persistent.queue

class InMemoryQueueWriteLayer<T> : QueueWriteLayer<T> {
    private var queue = mutableListOf<T>()

    override suspend fun push(t: List<T>) {
        queue.addAll(t)
    }

    override suspend fun peek(n: Int): List<T> {
        return queue.take(n).toList()
    }

    override suspend fun pop(n: Int) {
        queue = queue.drop(n).toMutableList()
    }

    override suspend fun size(): Int {
        return queue.size
    }

    override val concurrentReadWrites = false
}
