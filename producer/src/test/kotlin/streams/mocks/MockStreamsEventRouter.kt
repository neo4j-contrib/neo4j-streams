package streams.mocks

import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.mockito.Mockito
import org.neo4j.logging.internal.LogService
import org.neo4j.logging.internal.NullLogService
import streams.StreamsEventRouter
import streams.config.StreamsConfig
import streams.events.StreamsEvent
import streams.events.StreamsTransactionEvent
import streams.toMap

class MockStreamsEventRouter(logService: LogService = NullLogService.getInstance(),
                             config: StreamsConfig = Mockito.mock(StreamsConfig::class.java),
                             dbName: String = ""): StreamsEventRouter(logService, config, dbName) {

    private fun fakeRecordMetadata(topic: String) = RecordMetadata(
            TopicPartition(topic, 0),
            0, 1, 2, 3, 4, 5
    ).toMap()

    override fun sendEvents(topic: String, streamsTransactionEvents: List<out StreamsEvent>) {
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