package ai.whylabs.services.whylogs.util

import io.sentry.ITransaction
import io.sentry.Sentry
import io.sentry.SpanStatus

fun ITransaction.setException(t: Throwable) {
    throwable = t
    status = SpanStatus.INTERNAL_ERROR
    Sentry.captureException(t) // TODO Should this be here?
}


// TODO what is the name/operation??
inline fun <R> sentry(name: String, operation: String, block: (tr: ITransaction) -> R): R {
    val tr = Sentry.startTransaction(name, operation)
    try {
        return block(tr)
    } catch (t: Throwable) {
        tr.throwable = t
        tr.status = SpanStatus.INTERNAL_ERROR
        throw t
    } finally {
        tr.finish()
    }
}