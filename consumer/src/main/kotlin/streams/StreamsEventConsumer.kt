package streams

import org.neo4j.logging.Log
import streams.service.StreamsSinkEntity
import streams.service.dlq.DeadLetterQueueService


abstract class StreamsEventConsumer(private val log: Log, private val dlqService: DeadLetterQueueService?) {

    abstract fun stop()

    abstract fun withTopics(topics: Set<String>): StreamsEventConsumer

    abstract fun start()

    abstract fun read(topicConfig: Map<String, Any> = emptyMap(), action: (String, List<StreamsSinkEntity>) -> Unit)

    abstract fun read(action: (String, List<StreamsSinkEntity>) -> Unit)

}


abstract class StreamsEventConsumerFactory {
    abstract fun createStreamsEventConsumer(config: Map<String, String>, log: Log): StreamsEventConsumer
}