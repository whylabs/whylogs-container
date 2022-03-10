package ai.whylabs.services.whylogs.util

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.isActive
import kotlinx.coroutines.yield
import org.slf4j.Logger

suspend inline fun CoroutineScope.repeatUntilCancelled(log: Logger, block: () -> Unit) {
    while (isActive) {
        try {
            block()
            yield()
        } catch (e: TimeoutCancellationException) {
            // This doesn't actually count towards cancelling. We only want explicit cancels to
            // break this loop.
            log.info("coroutine on ${Thread.currentThread().name} timed out")
        } catch (ex: CancellationException) {
            log.info("coroutine on ${Thread.currentThread().name} cancelled")
            throw ex
        } catch (ex: Throwable) {
            log.error("${Thread.currentThread().name} failed. Retrying...", ex)
        }
    }

    log.info("coroutine on ${Thread.currentThread().name} exiting")
}
