package streams.events


class StreamsEventMetaBuilder(){

    private var timestamp: Long? = null
    private var username: String? = null
    private var txId: Long? = null
    private var txEventId: Int? = null
    private var txEventsCount: Int? = null
    private var operation: OperationType? = null
    private var source: MutableMap<String, Any> = mutableMapOf()

    fun withTimestamp(timestamp : Long) : StreamsEventMetaBuilder{
        this.timestamp = timestamp
        return this
    }

    fun withUsername(username : String) : StreamsEventMetaBuilder{
        this.username = username
        return this
    }

    fun withTransactionId(txId : Long) : StreamsEventMetaBuilder {
        this.txId = txId
        return this
    }

    fun withTransactionEventId(txEventId : Int) : StreamsEventMetaBuilder{
        this.txEventId = txEventId
        return this
    }

    fun withTransactionEventsCount(txEventsCount : Int) : StreamsEventMetaBuilder {
        this.txEventsCount = txEventsCount
        return this
    }

    fun withOperation(op : OperationType ) : StreamsEventMetaBuilder {
        this.operation = op
        return this
    }

    fun withSource(key : String, value : Any) : StreamsEventMetaBuilder{
        this.source.put(key, value)
        return this
    }

    fun build() : Meta{
        return Meta(timestamp!!, username!!, txId!!, txEventId!!, txEventsCount!!, operation!!, source)
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