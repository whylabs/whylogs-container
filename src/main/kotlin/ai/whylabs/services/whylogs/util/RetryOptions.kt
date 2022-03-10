package ai.whylabs.services.whylogs.util

import com.github.michaelbull.retry.policy.RetryPolicy
import com.github.michaelbull.retry.policy.fullJitterBackoff
import com.github.michaelbull.retry.policy.limitAttempts
import com.github.michaelbull.retry.policy.plus
import com.github.michaelbull.retry.retry

val defaultRetryPolicy: RetryPolicy<Throwable> = limitAttempts(5) + fullJitterBackoff(base = 10, max = 1000)

interface RetryOptions {
    val retryPolicy: RetryPolicy<Throwable>?
        get() = defaultRetryPolicy

    suspend fun <T> retryIfEnabled(block: suspend () -> T): T {
        val policy = this.retryPolicy
        return if (policy == null) {
            block()
        } else {
            retry(policy) { block() }
        }
    }
}
