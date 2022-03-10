package ai.whylabs.services.whylogs.persistent.map

class InMemoryMapWriteLayer<K, V> : MapWriteLayer<K, V> {
    private var map = mutableMapOf<K, V>()

    override suspend fun set(key: K, value: V) {
        map[key] = value
    }

    override suspend fun get(key: K): V? {
        return map[key]
    }

    override suspend fun getAll(): Map<K, V> {
        return map
    }

    override suspend fun reset(to: Map<K, V>) {
        map = to.toMutableMap()
    }

    override suspend fun remove(key: K) {
        map.remove(key)
    }

    override suspend fun size(): Int {
        return map.size
    }

    override suspend fun clear() {
        map.clear()
    }
}
