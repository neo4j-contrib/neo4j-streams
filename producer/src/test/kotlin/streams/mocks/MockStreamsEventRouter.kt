package streams.mocks

import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.logging.LogService
import streams.StreamsEventRouter
import streams.events.StreamsEvent
import streams.events.StreamsTransactionEvent
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Future

class MockStreamsEventRouter(logService: LogService? = null, config: Config? = null): StreamsEventRouter(logService, config) {

    fun fakeRecordMetadata(topic: String): RecordMetadata {
        return RecordMetadata(
                TopicPartition(topic, 0),
                0, 1, 2, 3, 4, 5)
    }

    override fun sendEvents(topic: String, streamsTransactionEvents: List<out StreamsEvent>): List<out Future<RecordMetadata>> {
        events.addAll(streamsTransactionEvents as List<StreamsTransactionEvent>)

        val result = mutableListOf<Future<RecordMetadata>>()
        streamsTransactionEvents.forEach {
            val future = CompletableFuture<RecordMetadata>()
            future.complete(fakeRecordMetadata(topic))
            result.add(future)
        }

        return result
    }

    override fun sendEventsSync(topic: String, transactionEvents: List<out StreamsEvent>): List<out RecordMetadata> {
        var result = mutableListOf<RecordMetadata>()

        transactionEvents.forEach {
            result.add(fakeRecordMetadata(topic))
        }

        return result
    }

    override fun start() {}

    override fun stop() {}

    companion object {
        var events = mutableListOf<StreamsTransactionEvent>()

        fun reset() {
            events.clear()
        }
    }

}