package streams.mocks

import org.mockito.Mockito
import org.neo4j.configuration.Config
import org.neo4j.logging.internal.LogService
import org.neo4j.logging.internal.NullLogService
import streams.StreamsEventRouter
import streams.config.StreamsConfig
import streams.events.StreamsEvent
import streams.events.StreamsTransactionEvent

class MockStreamsEventRouter(logService: LogService = NullLogService.getInstance(),
                             config: StreamsConfig = Mockito.mock(StreamsConfig::class.java),
                             dbName: String = ""): StreamsEventRouter(logService, config, dbName) {

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