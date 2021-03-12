package streams

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.logging.Log
import streams.events.StreamsEvent


abstract class StreamsEventRouter(config: Map<String, String>, db: GraphDatabaseService, log: Log) {

    abstract val eventRouterConfiguration: StreamsEventRouterConfiguration

    abstract fun sendEvents(topic: String, transactionEvents: List<out StreamsEvent>, config: Map<String, Any?> = emptyMap())

    abstract fun sendEventsSync(topic: String, transactionEvents: List<out StreamsEvent>, config: Map<String, Any?> = emptyMap()): List<Map<String, Any>>

    abstract fun start()

    abstract fun stop()

    open fun printInvalidTopics() {}

}


object StreamsEventRouterFactory {
    fun getStreamsEventRouter(config: Map<String, String>, db: GraphDatabaseService, log: Log): StreamsEventRouter {
        return Class.forName(config.getOrDefault("streams.router", "streams.kafka.KafkaEventRouter"))
                .getConstructor(Map::class.java, GraphDatabaseService::class.java, Log::class.java)
                .newInstance(config, db, log) as StreamsEventRouter
    }
}

