package streams.mocks

import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.neo4j.logging.Log
import org.neo4j.logging.NullLog
import streams.StreamsEventRouter
import streams.StreamsEventRouterConfiguration
import streams.events.StreamsEvent
import streams.events.StreamsPluginStatus
import streams.events.StreamsTransactionEvent
import streams.toMap
import java.util.concurrent.atomic.AtomicReference

class MockStreamsEventRouter(map: Map<String, String> = emptyMap(),
                             log: Log = NullLog.getInstance()): StreamsEventRouter(map, log) {

    override val eventRouterConfiguration: StreamsEventRouterConfiguration = StreamsEventRouterConfiguration()

    private var status = AtomicReference<StreamsPluginStatus>()

    private fun fakeRecordMetadata(topic: String) = RecordMetadata(
            TopicPartition(topic, 0),
            0, 1, 2, 3, 4, 5
    ).toMap()

    override fun sendEvents(topic: String, streamsTransactionEvents: List<out StreamsEvent>, config: Map<String, Any?>) {
        events.addAll(streamsTransactionEvents as List<StreamsTransactionEvent>)
    }

    override fun sendEventsSync(topic: String, streamsTransactionEvents: List<out StreamsEvent>, config: Map<String, Any?>): List<Map<String, Any>> {
        val result = mutableListOf<Map<String, Any>>()
        streamsTransactionEvents.forEach {
            result.add(fakeRecordMetadata(topic))
        }
        return result
    }

    override fun start() {
        status.set(StreamsPluginStatus.RUNNING)
    }

    override fun stop() {
        status.set(StreamsPluginStatus.STOPPED)
    }

    companion object {
        val events = mutableListOf<StreamsTransactionEvent>()

        fun reset() {
            events.clear()
        }
    }

}