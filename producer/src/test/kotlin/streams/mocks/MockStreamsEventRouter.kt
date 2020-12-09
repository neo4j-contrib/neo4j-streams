package streams.mocks

import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.neo4j.kernel.configuration.Config
import org.neo4j.logging.internal.LogService
import streams.StreamsEventRouter
import streams.events.StreamsEvent
import streams.events.StreamsTransactionEvent
import streams.toMap

class MockStreamsEventRouter(logService: LogService? = null, config: Config? = null): StreamsEventRouter(logService, config) {

    private fun fakeRecordMetadata(topic: String) = RecordMetadata(
            TopicPartition(topic, 0),
            0, 1, 2, 3, 4, 5
    ).toMap()

    override fun sendEvents(topic: String, streamsTransactionEvents: List<out StreamsEvent>, config: Map<String, Any?>) {
        events.addAll(streamsTransactionEvents as List<StreamsTransactionEvent>)
    }

    override fun sendEventsSync(topic: String, streamsTransactionEvents: List<out StreamsEvent>): List<Map<String, Any>> {
        val result = mutableListOf<Map<String, Any>>()
        streamsTransactionEvents.forEach {
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