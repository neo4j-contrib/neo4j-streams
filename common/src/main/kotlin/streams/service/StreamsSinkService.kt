package streams.service

import streams.serialization.JSONUtils


const val STREAMS_TOPIC_KEY: String = "streams.sink.topic"
const val STREAMS_TOPIC_CDC_KEY: String = "streams.sink.topic.cdc"

enum class TopicType(val isCDC: Boolean = false, val key: String) {
    CDC_MERGE(true, "$STREAMS_TOPIC_CDC_KEY.merge"),
    CYPHER(key = "$STREAMS_TOPIC_KEY.cypher"),
    CDC_SCHEMA(true, "$STREAMS_TOPIC_CDC_KEY.schema")
}

abstract class StreamsSinkService {

    abstract fun getTopicType(topic: String): TopicType?
    abstract fun getCypherTemplate(topic: String): String?
    abstract fun write(query: String, events: Collection<Any>)

    private fun writeCDCTopic(params: Collection<Any>, strategy: CDCQueryStrategy) {
        val data = params
                .map { JSONUtils.asStreamsTransactionEvent(it) }

        strategy.mergeNodeEvents(data).forEach { write(it.query, it.events) }
        strategy.deleteNodeEvents(data).forEach { write(it.query, it.events) }

        strategy.mergeRelationshipEvents(data).forEach { write(it.query, it.events) }
        strategy.deleteRelationshipEvents(data).forEach { write(it.query, it.events) }
    }

    private fun writeWithCypherTemplate(topic: String, params: Collection<Any>) {
        val query = getCypherTemplate(topic) ?: return
        write(query, params)
    }

    fun writeForTopic(topic: String, params: Collection<Any>) {
        when (getTopicType(topic)) {
            TopicType.CYPHER -> writeWithCypherTemplate(topic, params)
            TopicType.CDC_MERGE -> writeCDCTopic(params, MergeCDCQueryStrategy())
            TopicType.CDC_SCHEMA -> writeCDCTopic(params, SchemaCDCQueryStrategy())
            else -> throw RuntimeException("Unknown topic type")
        }
    }
}