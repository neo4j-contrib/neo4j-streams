package streams

import streams.events.StreamsEvent

interface StreamsEventRouter {

    fun sendEvent(event : StreamsEvent)

}