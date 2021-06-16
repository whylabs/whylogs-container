package ai.whylabs.services.whylogs.persistent.queue

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.actor
import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory
import java.util.concurrent.Executors
import kotlin.math.min

private val logger = LoggerFactory.getLogger("queueMessageHandler")

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

private fun <T> messageHandler(options: QueueOptions<T>): MessageHandler<T> {
    return if (options.writeLayer.concurrentReadWrites()) {
        concurrentMessageHandler(options)
    } else {
        serialMessageHandler(options)
    }
}

private fun <T> concurrentMessageHandler(options: QueueOptions<T>): MessageHandler<T> {
    val pushActor = subActor<PersistentQueueMessage.PushMessage<T>> { msg ->
        try {
            push(options, msg)
        } catch (t: Throwable) {
            logger.error("Error pushing items", t)
            msg.done.completeExceptionally(t)
        }
    }

    val popActor = subActor<PersistentQueueMessage.PopMessage<T>> { msg ->
        try {
            pop(options, msg)
        } catch (t: Throwable) {
            logger.error("Error popping items", t)
            msg.done.completeExceptionally(t)
        }
    }

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


@ObsoleteCoroutinesApi
internal fun <T> queueMessageHandler(options: QueueOptions<T>) =
    options.scope.actor<PersistentQueueMessage<T>>(capacity = Channel.UNLIMITED) {
        val handler = messageHandler(options)
        for (msg in channel) {
            logger.debug("Handling message ${msg.javaClass}")
            try {
                handler(msg)
            } catch (t: Throwable) {
                logger.error("Error popping items", t)
                msg.done.completeExceptionally(t)
            }

        }
    }

private fun <M : PersistentQueueMessage<*>> subActor(
    handler: suspend (M) -> Unit
): SendChannel<M> {
    val scope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
    return scope.actor(capacity = Channel.UNLIMITED) {
        for (msg in channel) {
            handler(msg)
        }
    }
}


private suspend fun <T> push(options: QueueOptions<T>, message: PersistentQueueMessage.PushMessage<T>) {
    withTimeout(3000) {
        logger.debug("Writing message")
        options.writeLayer.push(message.value)
        message.done.complete(Unit)
        logger.debug("Done writing message")
    }
}

private suspend fun <T> pop(options: QueueOptions<T>, message: PersistentQueueMessage.PopMessage<T>) {
    withTimeout(30_000) {
        val popSize = when (message.n) {
            is PopSize.All -> options.writeLayer.size()
            is PopSize.N -> min(message.n.n, options.writeLayer.size())
        }

        if (popSize == 0) {
            logger.debug("Nothing in the queue to pop")
            message.items.cancel("No items to process")
            message.done.complete(Unit)
            return@withTimeout
        }

        logger.debug("Peeking $popSize messages")
        val items = options.writeLayer.peek(popSize)
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
        options.writeLayer.pop(popSize)

        // Done with everything
        logger.debug("Done with everything")
        message.done.complete(Unit)
    }
}
