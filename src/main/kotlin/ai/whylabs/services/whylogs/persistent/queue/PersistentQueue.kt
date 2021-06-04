package ai.whylabs.services.whylogs.persistent.queue

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import org.slf4j.LoggerFactory
import java.util.concurrent.CancellationException
import java.util.concurrent.Executors

class PersistentQueue<T>(writer: WriteLayer<T>) : AutoCloseable {
    private val logger = LoggerFactory.getLogger(javaClass)

    private val act =
        queueMessageHandler(
            QueueOptions(
                CoroutineScope(Executors.newFixedThreadPool(3).asCoroutineDispatcher()),
                writer
            )
        )


    suspend fun push(items: List<T>) {
        logger.debug("Pushing")
        val done = CompletableDeferred<Unit>()
        logger.debug("Sending message and waiting for done signal")
        act.send(PersistentQueueMessage.PushMessage(done, items))
        done.await()
        logger.debug("Done pushing")
    }

    suspend fun pop(count: PopSize, block: suspend (List<T>) -> Unit) {
        val done = CompletableDeferred<Unit>()
        val processingDone = CompletableDeferred<Unit>()
        val onItems = CompletableDeferred<List<T>>()

        logger.debug("Popping")
        act.send(PersistentQueueMessage.PopMessage(done, onItems, processingDone, count))

        logger.debug("Waiting for item lookup.")
        try {
            val items = onItems.await()
            try {
                logger.debug("Calling the processing block")
                block(items)
                processingDone.complete(Unit)
            } catch (t: Throwable) {
                logger.error("Error in processing block", t)
                processingDone.completeExceptionally(t)
            }
            logger.debug("Waiting for the final done signal")
            done.await()
            logger.debug("Done popping.")
        } catch (e: CancellationException) {
            // If there is nothing to process then we get a cancel.
            logger.debug("Nothing to process.")
            done.await()
        }
    }

    override fun close() {
        act.close()
    }
}

sealed class PopSize {
    class N(val n: Int) : PopSize()
    object All : PopSize()
}

data class QueueOptions<T>(
    internal val scope: CoroutineScope,
    internal val writeLayer: WriteLayer<T>,
)


