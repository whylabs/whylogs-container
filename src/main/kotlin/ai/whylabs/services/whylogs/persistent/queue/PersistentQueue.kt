package ai.whylabs.services.whylogs.persistent.queue

import ai.whylabs.services.whylogs.util.RetryOptions
import ai.whylabs.services.whylogs.util.defaultRetryPolicy
import com.github.michaelbull.retry.policy.RetryPolicy
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory

class PersistentQueue<T>(options: QueueOptions<T>) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val scope = CoroutineScope(Dispatchers.IO)
    private val act = scope.queueMessageHandler(options)

    suspend fun push(items: List<T>) {
        withContext(scope.coroutineContext) {
            logger.debug("Pushing")
            val done = CompletableDeferred<Unit>()
            logger.debug("Sending message and waiting for done signal")
            act.send(PersistentQueueMessage.PushMessage(done, items))
            done.await()
        }
    }

    suspend fun pop(count: PopSize, block: suspend (List<T>) -> Unit) {
        withContext(scope.coroutineContext) {
            val done = CompletableDeferred<Unit>()
            val processingDone = CompletableDeferred<Unit>()
            val onItems = CompletableDeferred<List<T>>()

            logger.debug("Popping")
            act.send(PersistentQueueMessage.PopMessage(done, onItems, processingDone, count))

            logger.debug("Waiting for item lookup.")
            try {
                val items = onItems.await()
                logger.debug("Calling the processing block")
                block(items)
                processingDone.complete(Unit)
                logger.debug("Done popping.")
            } catch (t: Throwable) {
                logger.error("Error in processing block", t)
                processingDone.completeExceptionally(t)
            } finally {
                logger.debug("Waiting for the final done signal")
                done.await()
            }
        }
    }

    suspend fun popUntilEmpty(incrementSize: PopSize, block: suspend (List<T>) -> Unit) {
        withContext(scope.coroutineContext) {
            var empty = false
            while (!empty) {
                // TODO make this configurable. This is how fast the queue is drained. If this number is low/0 then
                // you'll see lots of small transactions as requests trickle in. If its large then you'll see fewer
                // transactions but they might take a big longer to totally finish. 100ms was a sweet spot for tops in
                // some local testing with small payloads but this really depends on hardware.
                delay(100)
                pop(incrementSize) {
                    empty = it.isEmpty()
                    block(it)
                }
            }
        }
    }
}

sealed class PopSize {
    class N(val n: Int) : PopSize()
    object All : PopSize()
}

data class QueueOptions<T>(
    val queueWriteLayer: QueueWriteLayer<T>,
    override val retryPolicy: RetryPolicy<Throwable>? = defaultRetryPolicy
) : RetryOptions
