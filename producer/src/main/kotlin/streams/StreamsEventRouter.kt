package streams

import org.neo4j.logging.internal.LogService
import streams.config.StreamsConfig
import streams.events.StreamsEvent


abstract class StreamsEventRouter(val logService: LogService, val config: StreamsConfig, val dbName: String) {

    abstract fun sendEvents(topic: String, transactionEvents: List<out StreamsEvent>)

    abstract fun sendEventsSync(topic: String, transactionEvents: List<out StreamsEvent>): List<Map<String, Any>>

    abstract fun start()

    abstract fun stop()

    open fun printInvalidTopics() {}

}


object StreamsEventRouterFactory {
    fun getStreamsEventRouter(logService: LogService, config: StreamsConfig, dbName: String): StreamsEventRouter {
        return Class.forName(config.config.getOrDefault("streams.router", "streams.kafka.KafkaEventRouter"))
                .getConstructor(LogService::class.java, StreamsConfig::class.java, String::class.java)
                .newInstance(logService, config, dbName) as StreamsEventRouter
    }
}

