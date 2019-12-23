package streams.mocks

import org.neo4j.configuration.Config
import org.neo4j.logging.internal.LogService
import org.neo4j.logging.internal.NullLogService
import streams.StreamsEventRouter
import streams.events.StreamsEvent
import streams.events.StreamsTransactionEvent

class MockStreamsEventRouter(logService: LogService = NullLogService.getInstance(),
                             config: Map<String, String> = emptyMap()): StreamsEventRouter(logService, config) {

    override fun sendEvents(topic: String, streamsTransactionEvents: List<out StreamsEvent>) {
        events.addAll(streamsTransactionEvents as List<StreamsTransactionEvent>)
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