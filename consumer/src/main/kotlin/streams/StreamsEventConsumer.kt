package streams

import org.neo4j.logging.Log
import streams.service.StreamsSinkEntity
import streams.service.errors.ErrorService


abstract class StreamsEventConsumer(private val log: Log, private val dlqService: ErrorService) {

    abstract fun stop()

    abstract fun withTopics(topics: Set<String>): StreamsEventConsumer

    abstract fun start()

    abstract fun read(topicConfig: Map<String, Any> = emptyMap(), action: (String, List<StreamsSinkEntity>) -> Unit)

    abstract fun read(action: (String, List<StreamsSinkEntity>) -> Unit)

    abstract fun invalidTopics(): List<String>

}


abstract class StreamsEventConsumerFactory {
    abstract fun createStreamsEventConsumer(config: Map<String, String>, log: Log): StreamsEventConsumer
}