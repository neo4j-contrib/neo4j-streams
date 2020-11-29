package streams.mocks

import org.neo4j.logging.Log
import org.neo4j.logging.NullLog
import streams.StreamsEventRouter
import streams.StreamsEventRouterConfiguration
import streams.events.StreamsEvent
import streams.events.StreamsPluginStatus
import streams.events.StreamsTransactionEvent
import java.util.concurrent.atomic.AtomicReference

class MockStreamsEventRouter(map: Map<String, String> = emptyMap(),
                             log: Log = NullLog.getInstance()): StreamsEventRouter(map, log) {

    override val eventRouterConfiguration: StreamsEventRouterConfiguration = StreamsEventRouterConfiguration()

    private var status = AtomicReference<StreamsPluginStatus>()

    override fun sendEvents(topic: String, streamsTransactionEvents: List<out StreamsEvent>) {
        events.addAll(streamsTransactionEvents as List<StreamsTransactionEvent>)
    }

    override fun start() {
        status.set(StreamsPluginStatus.RUNNING)
    }

    override fun stop() {
        status.set(StreamsPluginStatus.STOPPED)
    }

    override fun status(): StreamsPluginStatus = status.get()

    companion object {
        var events = mutableListOf<StreamsTransactionEvent>()

        fun reset() {
            events.clear()
        }
    }

}