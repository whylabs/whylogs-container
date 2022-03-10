package ai.whylabs.services.whylogs.kafka

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.yield
import org.slf4j.Logger

suspend inline fun CoroutineScope.repeatUntilCancelled(log: Logger, block: () -> Unit) {
    while (isActive) {
        try {
            block()
            yield()
        } catch (ex: CancellationException) {
            log.info("coroutine on ${Thread.currentThread().name} cancelled")
        } catch (ex: Throwable) {
            log.error("${Thread.currentThread().name} failed. Retrying...", ex)
            delay(2_000)
        }
    }

    log.info("coroutine on ${Thread.currentThread().name} exiting")
}
