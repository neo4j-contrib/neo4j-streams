package streams.utils

import kotlinx.coroutines.delay

suspend fun <T> retryForException(exceptions: Array<Class<out Throwable>>, retries: Int, delayTime: Long, action: () -> T): T {
    return try {
        action()
    } catch (e: Exception) {
        val isInstance = exceptions.any { it.isInstance(e) }
        if (isInstance && retries > 0) {
            delay(delayTime)
            retryForException(exceptions = exceptions, retries = retries - 1, delayTime = delayTime, action = action)
        } else {
            throw e
        }
    }
}