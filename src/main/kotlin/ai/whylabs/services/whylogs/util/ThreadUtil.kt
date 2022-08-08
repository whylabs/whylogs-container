package ai.whylabs.services.whylogs.util

/**
 * Thread.sleep until some condition is met.
 * This shouldn't be used from coroutines, only java threads.
 */
inline fun waitUntil(block: () -> Boolean) {
    while (!block()) {
        Thread.sleep(100)
    }
}
