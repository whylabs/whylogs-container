package ai.whylabs.services.whylogs.persistent.queue

class MockWriteLayer<T> : WriteLayer<T> {
    var storage = mutableListOf<T>()

    override suspend fun push(t: List<T>) {
        storage.addAll(t)
    }

    override suspend fun peek(n: Int): List<T> {

        return storage.take(n)
    }

    override suspend fun pop(n: Int) {
        storage = if (n > storage.size) {
            mutableListOf()
        } else {
            storage.takeLast(storage.size - n).toMutableList()
        }
    }

    override suspend fun size() = storage.size

    override fun concurrentReadWrites() = false
}
