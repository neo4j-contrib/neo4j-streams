package streams.utils

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.delay

object StreamsUtils {

    @JvmStatic val UNWIND: String = "UNWIND \$events AS event"

    @JvmStatic val STREAMS_CONFIG_PREFIX = "streams."

    @JvmStatic val STREAMS_SINK_TOPIC_PREFIX = "sink.topic.cypher."

    @JvmStatic val LEADER = "LEADER"

    @JvmStatic val SYSTEM_DATABASE_NAME = "system"

    fun <T> ignoreExceptions(action: () -> T, vararg toIgnore: Class<out Throwable>): T? {
        return try {
            action()
        } catch (e: Throwable) {
            if (toIgnore.isEmpty()) {
                return null
            }
            return when (e::class.java) {
                in toIgnore -> null
                else -> throw e
            }
        }
    }

    fun blockUntilTrueOrTimeout(timeout: Long, delay: Long = 1000, action: () -> Boolean): Boolean = runBlocking {
        val start = System.currentTimeMillis()
        var success = action()
        while (System.currentTimeMillis() - start < timeout && !success) {
            delay(delay)
            success = action()
        }
        success
    }

}