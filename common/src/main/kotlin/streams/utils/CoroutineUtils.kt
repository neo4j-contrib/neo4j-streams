package streams.utils

import kotlinx.coroutines.delay

suspend fun <T> retryForException(exceptions: Array<Class<out Throwable>>, retries: Int, delayTime: Long, excludeExceptions: List<String>? = null, action: () -> T): T {
    println("action() + $action")
    return try {
        action()
    } catch (e: Exception) {
         
        // todo - provare con excludeExceptions nullo
        val b = e.message?.let { msg -> excludeExceptions?.none { msg.contains(it) } } ?: true
//        val b = true
        val isInstance = b && exceptions.any { it.isInstance(e) }
        if (isInstance && retries > 0) {
            println("retry exc - $e")
            delay(delayTime)
            retryForException(exceptions = exceptions, retries = retries - 1, delayTime = delayTime, action = action)
        } else {
            throw e
        }
    }
}