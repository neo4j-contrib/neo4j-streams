package streams.events

import kafka.UpdateState

class StreamsEventMetaBuilder(){

    private var timestamp: Long? = null
    private var username: String? = null
    private var txId: Long? = null
    private var txEventId: Int? = null
    private var txEventsCount: Int? = null
    private var operation: UpdateState? = null
    private var source: Map<String, Any> = emptyMap()


    fun withTimestamp(timestamp : Long) : StreamsEventMetaBuilder{
        this.timestamp = timestamp
        return this
    }

    //TODO...

    fun build() : Meta{
        return Meta(timestamp!!, username!!, txId!!, txEventId!!, txEventsCount!!, operation!!, source!!)
    }

}


class StreamsEventBuilder(){

    private var meta: Meta? = null
    private var payload: Payload? = null
    private var schema: Schema? = null

    fun withMeta(meta : Meta): StreamsEventBuilder{
        this.meta = meta
        return this
    }

    fun build() : StreamsEvent{
        return StreamsEvent(meta!!, payload!!, schema!!)
    }
}