package streams

import org.neo4j.kernel.configuration.Config
import org.neo4j.logging.internal.LogService
import streams.events.StreamsEvent


abstract class StreamsEventRouter(val logService: LogService?, val config: Config?) {

    abstract fun sendEvents(topic: String, transactionEvents: List<out StreamsEvent>)

    abstract fun start()

    abstract fun stop()

    open fun printInvalidTopics() {}

}


object StreamsEventRouterFactory {
    fun getStreamsEventRouter(logService: LogService, config: Config): StreamsEventRouter {
        return Class.forName(config.raw.getOrDefault("streams.router", "streams.kafka.KafkaEventRouter"))
                .getConstructor(LogService::class.java, Config::class.java)
                .newInstance(logService, config) as StreamsEventRouter
    }
}

