package streams.utils

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.delay
import org.neo4j.kernel.internal.GraphDatabaseAPI

object StreamsUtils {

    const val UNWIND: String = "UNWIND {events} AS event"

    const val STREAMS_CONFIG_PREFIX = "streams."

    const val STREAMS_SINK_TOPIC_PREFIX = "sink.topic.cypher."

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

    fun blockUntilFalseOrTimeout(timeout: Long, delay: Long = 1000, action: () -> Boolean): Boolean = runBlocking {
        val start = System.currentTimeMillis()
        var success = action()
        while (System.currentTimeMillis() - start < timeout && !success) {
            delay(delay)
            success = action()
        }
        success
    }

    fun getName(db: GraphDatabaseAPI) = db.databaseLayout().databaseDirectory().absolutePath

}