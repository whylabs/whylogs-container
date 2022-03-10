package ai.whylabs.services.whylogs.persistent.queue

import ai.whylabs.services.whylogs.util.LoggingUtil.getLoggerForFile
import ai.whylabs.services.whylogs.util.repeatUntilCancelled
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.actor
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.newSingleThreadContext
import kotlinx.coroutines.withTimeout
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.math.min

private val logger = getLoggerForFile()

internal sealed class PersistentQueueMessage<T>(val done: CompletableDeferred<Unit>) {

    class PushMessage<T>(done: CompletableDeferred<Unit>, val value: List<T>) : PersistentQueueMessage<T>(done)
    class PopMessage<T>(
        done: CompletableDeferred<Unit>,
        val items: CompletableDeferred<List<T>>,
        val processingDone: CompletableDeferred<Unit>,
        val n: PopSize
    ) : PersistentQueueMessage<T>(done)
}

private typealias MessageHandler <T> = suspend (message: PersistentQueueMessage<T>) -> Unit

private fun <T> CoroutineScope.messageHandler(options: QueueOptions<T>): MessageHandler<T> {
    return if (options.queueWriteLayer.concurrentPushPop) concurrentMessageHandler(options) else serialMessageHandler(options)
}

private fun <T> CoroutineScope.concurrentMessageHandler(options: QueueOptions<T>): MessageHandler<T> {
    // Dedicating a single thread to each of these paths is an extra layer of concurrency guarantees.
    // If they shared a common pool then coroutines might have both of them running on the same thread
    // across some period of time, which would be bad if they both used sqlite connections, for example,
    // which are supposed to be unique to a each thread. They still inherit the scope of the parent coroutine
    // but they run on the context's thread, so they share cancellation with the parent.
    val pushActor = subActor<PersistentQueueMessage.PushMessage<T>>(newSingleThreadContext("push-actor")) { push(options, it) }
    val popActor = subActor<PersistentQueueMessage.PopMessage<T>>(newSingleThreadContext("pop-actor")) { pop(options, it) }

    return { msg ->
        when (msg) {
            is PersistentQueueMessage.PushMessage<T> -> pushActor.send(msg)
            is PersistentQueueMessage.PopMessage<T> -> popActor.send(msg)
        }
    }
}

private fun <T> serialMessageHandler(options: QueueOptions<T>): MessageHandler<T> {
    return { msg ->
        when (msg) {
            is PersistentQueueMessage.PushMessage<T> -> push(options, msg)
            is PersistentQueueMessage.PopMessage<T> -> pop(options, msg)
        }
    }
}

internal fun <T> CoroutineScope.queueMessageHandler(options: QueueOptions<T>) = actor<PersistentQueueMessage<T>>(capacity = 1000) {
    consumeUntilCancelled(channel, messageHandler(options))
}

private fun <M : PersistentQueueMessage<*>> CoroutineScope.subActor(context: CoroutineContext = EmptyCoroutineContext, handler: suspend (M) -> Unit) =
    actor<M>(capacity = 1000, context = context) {
        consumeUntilCancelled(channel, handler)
    }

private suspend fun <M : PersistentQueueMessage<*>> consumeUntilCancelled(channel: Channel<M>, handler: suspend (M) -> Unit) {
    coroutineScope {
        repeatUntilCancelled(logger) {
            for (msg in channel) {
                try {
                    handler(msg) // This thing has to do the error handling
                } catch (t: Throwable) {
                    logger.error("Error handling message $msg in channel.", t)
                    msg.done.completeExceptionally(t)
                }
            }
        }
    }
    logger.error("actor existed unexpectedly because the channel closed or it was cancelled, which shouldn't be possible.")
}

private suspend fun <T> push(options: QueueOptions<T>, message: PersistentQueueMessage.PushMessage<T>) {
    withTimeout(10_000) {
        logger.debug("Writing message")

        options.retryIfEnabled {
            options.queueWriteLayer.push(message.value)
        }

        message.done.complete(Unit)
        logger.debug("Done writing message")
    }
}

private suspend fun <T> pop(options: QueueOptions<T>, message: PersistentQueueMessage.PopMessage<T>) {
    withTimeout(30_000) {
        val popSize = when (message.n) {
            is PopSize.All -> options.retryIfEnabled { options.queueWriteLayer.size() }
            is PopSize.N -> min(message.n.n, options.queueWriteLayer.size())
        }

        if (popSize == 0) {
            logger.debug("Nothing in the queue to pop.")
            message.items.complete(emptyList())
            message.done.complete(Unit)
            return@withTimeout
        }

        logger.debug("Peeking $popSize messages")
        val items = options.retryIfEnabled {
            options.queueWriteLayer.peek(popSize)
        }
        message.items.complete(items)

        logger.debug("Waiting for items to be processed by the consumer")
        try {
            message.processingDone.await()
        } catch (t: Throwable) {
            logger.error("Error while waiting for processor, bailing out without any side effects.")
            message.done.completeExceptionally(t)
            return@withTimeout
        }

        // means that the items were handled and they can be discarded now
        logger.debug("Discarding processed items")
        options.retryIfEnabled { options.queueWriteLayer.pop(popSize) }

        // Done with everything
        logger.debug("Done with everything")
        message.done.complete(Unit)
    }
}
