package streams

import org.apache.kafka.clients.producer.RecordMetadata
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.logging.LogService
import streams.events.StreamsEvent
import java.util.concurrent.Future


abstract class StreamsEventRouter(val logService: LogService?, val config: Config?) {

    abstract fun sendEvents(topic: String, transactionEvents: List<out StreamsEvent>): List<out Future<RecordMetadata>>

    abstract fun sendEventsSync(topic: String, transactionEvents: List<out StreamsEvent>): List<out RecordMetadata>

    abstract fun start()

    abstract fun stop()

}


object StreamsEventRouterFactory {
    fun getStreamsEventRouter(logService: LogService, config: Config): StreamsEventRouter {
        return Class.forName(config.raw.getOrDefault("streams.router", "streams.kafka.KafkaEventRouter"))
                .getConstructor(LogService::class.java, Config::class.java)
                .newInstance(logService, config) as StreamsEventRouter
    }
}

