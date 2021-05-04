package streams.utils

import kotlinx.coroutines.delay

suspend fun <T> retryForException(exceptions: Array<Class<out Throwable>>, retries: Int, delayTime: Long, excludeExceptions: List<String>? = null, action: () -> T): T {
    return try {
        action()
    } catch (e: Exception) {
        val b = e.message?.let { msg -> excludeExceptions?.none { msg.contains(it) } } ?: true
        val isInstance = b && exceptions.any { it.isInstance(e) }
        if (isInstance && retries > 0) {
            delay(delayTime)
            retryForException(exceptions = exceptions, retries = retries - 1, delayTime = delayTime, action = action)
        } else {
            throw e
        }
    }
}