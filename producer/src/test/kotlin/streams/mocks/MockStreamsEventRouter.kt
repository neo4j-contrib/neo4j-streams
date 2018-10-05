package streams.mocks

import streams.StreamsEventRouter
import streams.events.StreamsEvent

class MockStreamsEventRouter : StreamsEventRouter {

    var events = mutableListOf<StreamsEvent>()

    override fun sendEvent(event: StreamsEvent) {
        this.events.add(event)
    }

}