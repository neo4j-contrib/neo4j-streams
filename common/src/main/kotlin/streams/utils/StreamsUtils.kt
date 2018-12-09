package streams.utils

object StreamsUtils {

    const val UNWIND: String = "UNWIND {events} AS event"

    const val STREAMS_CONFIG_PREFIX = "streams."

    const val STREAMS_SINK_TOPIC_PREFIX = "sink.topic.cypher."

    fun <T> ignoreExceptions(action: () -> T, vararg toIgnore: Class<out Throwable>): T? {
        return try {
            action()
        } catch (e: Throwable) {
            when (e::class.java) {
                in toIgnore -> null
                else -> throw e
            }
        }
    }

}