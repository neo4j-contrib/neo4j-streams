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

    fun withHostname(host : String) : StreamsEventMetaBuilder{
        this.source.put("hostname", host)
        return this
    }

    fun build() : Meta{
        return Meta(timestamp!!, username!!, txId!!, txEventId!!, txEventsCount!!, operation!!, source)
    }

}

class NodeChangeBuilder(){

    private var labels : List<String> = listOf()
    private var properties :  Map<String, Any> = mapOf()

    fun withLabels(labels : List<String>) : NodeChangeBuilder{
        this.labels = labels
        return this
    }

    fun withProperties(properties : Map<String, Any>) : NodeChangeBuilder{
        this.properties = properties
        return this
    }

    fun build() : NodeChange{
        return NodeChange(properties = properties, labels = labels)
    }
}

class NodePayloadBuilder(){

    private var id : String = "0"
    private var after : NodeChange? = null
    private var before : NodeChange? = null

    fun withId(id : String) : NodePayloadBuilder{
        this.id = id
        return this
    }

    fun withBefore(before : NodeChange) : NodePayloadBuilder{
        this.before = before
        return this
    }

    fun withAfter(after : NodeChange) : NodePayloadBuilder{
        this.after = after
        return this
    }

    fun build() : NodePayload{
        return NodePayload(id, before, after)
    }
}

class RelationshipChangeBuilder(){

    private var properties :  Map<String, Any> = mapOf()

    fun withProperties(properties : Map<String, Any>) : RelationshipChangeBuilder{
        this.properties = properties
        return this
    }

    fun build() : RelationshipChange{
        return RelationshipChange(properties= properties)
    }
}

class RelationshipPayloadBuilder() {
    private var id: String = "0"
    private var after: RelationshipChange? = null
    private var before: RelationshipChange? = null
    private var name: String? = null
    private var startNode : RelationshipNodeChange ? = null
    private var endNode : RelationshipNodeChange ? = null

    fun withStartNode(id: String, labels: List<String>): RelationshipPayloadBuilder{
        this.startNode = RelationshipNodeChange(id, labels)
        return this
    }

    fun withEndNode(id: String, labels: List<String>): RelationshipPayloadBuilder{
        this.endNode = RelationshipNodeChange(id, labels)
        return this
    }

    fun withId(id: String): RelationshipPayloadBuilder {
        this.id = id
        return this
    }

    fun withBefore(before: RelationshipChange): RelationshipPayloadBuilder {
        this.before = before
        return this
    }

    fun withAfter(after: RelationshipChange): RelationshipPayloadBuilder {
        this.after = after
        return this
    }

    fun withName(name: String): RelationshipPayloadBuilder {
        this.name = name
        return this
    }

    fun build(): RelationshipPayload {
        return RelationshipPayload(id = id, before = before, after = after, label = name!!, start = startNode!!, end = endNode!! )
    }
}

class SchemaBuilder() {

    fun build() : Schema{
        //FIXME implement
        return Schema()
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

    fun withPayload(payload : Payload) : StreamsEventBuilder{
        this.payload = payload
        return this
    }

    fun withSchema(schema : Schema) : StreamsEventBuilder{
        this.schema = schema
        return this
    }

    fun build() : StreamsTransactionEvent{
        return StreamsTransactionEvent(meta!!, payload!!, schema!!)
    }
}