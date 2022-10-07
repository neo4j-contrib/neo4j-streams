package streams.utils

object StreamsUtils {

    @JvmStatic val UNWIND: String = "UNWIND \$events AS event"

    @JvmStatic val WITH_EVENT_FROM: String = "WITH event, from"

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
            return if (toIgnore.any { it.isInstance(e) }) {
                null
            } else {
                throw e
            }
        }
    }

    fun closeSafetely(closeable: AutoCloseable, onError: (Throwable) -> Unit = {}) = try {
        closeable.close()
    } catch (e: Throwable) {
        onError(e)
    }

}