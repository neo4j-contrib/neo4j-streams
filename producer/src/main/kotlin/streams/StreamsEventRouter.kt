package streams

import org.neo4j.logging.internal.LogService
import streams.events.StreamsEvent


abstract class StreamsEventRouter(val logService: LogService, val config: Map<String, String>) {

    abstract fun sendEvents(topic: String, transactionEvents: List<out StreamsEvent>)

    abstract fun start()

    abstract fun stop()

    open fun printInvalidTopics() {}

}


object StreamsEventRouterFactory {
    fun getStreamsEventRouter(logService: LogService, config: Map<String, String>): StreamsEventRouter {
        return Class.forName(config.getOrDefault("streams.router", "streams.kafka.KafkaEventRouter"))
                .getConstructor(LogService::class.java, Map::class.java)
                .newInstance(logService, config) as StreamsEventRouter
    }
}

