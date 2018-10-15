package streams.mocks

import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.logging.LogService
import streams.StreamsEventRouter
import streams.events.StreamsEvent

class MockStreamsEventRouter(logService: LogService? = null, config: Config? = null): StreamsEventRouter(logService, config) {

    override fun sendEvent(event: StreamsEvent) {
        events.add(event)
    }

    companion object {
        var events = mutableListOf<StreamsEvent>()

        fun reset() {
            events.clear()
        }
    }

}