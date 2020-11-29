package streams

import org.neo4j.logging.Log
import streams.events.StreamsEvent
import streams.events.StreamsPluginStatus


abstract class StreamsEventRouter(config: Map<String, String>, log: Log) {

    abstract val eventRouterConfiguration: StreamsEventRouterConfiguration

    abstract fun sendEvents(topic: String, transactionEvents: List<out StreamsEvent>)

    abstract fun start()

    abstract fun stop()

    open fun printInvalidTopics() {}

    abstract fun status(): StreamsPluginStatus

}


object StreamsEventRouterFactory {
    fun getStreamsEventRouter(config: Map<String, String>,log: Log): StreamsEventRouter {
        return Class.forName(config.getOrDefault("streams.router", "streams.kafka.KafkaEventRouter"))
                .getConstructor(Map::class.java, Log::class.java)
                .newInstance(config, log) as StreamsEventRouter
    }
}

