package streams.service

import streams.service.sink.strategy.IngestionStrategy


const val STREAMS_TOPIC_KEY: String = "streams.sink.topic"
const val STREAMS_TOPIC_CDC_KEY: String = "streams.sink.topic.cdc"

enum class TopicTypeGroup { CYPHER, CDC, PATTERN, CUD }
enum class TopicType(val group: TopicTypeGroup, val key: String) {
    CDC_SOURCE_ID(group = TopicTypeGroup.CDC, key = "$STREAMS_TOPIC_CDC_KEY.sourceId"),
    CYPHER(group = TopicTypeGroup.CYPHER, key = "$STREAMS_TOPIC_KEY.cypher"),
    PATTERN_NODE(group = TopicTypeGroup.PATTERN, key = "$STREAMS_TOPIC_KEY.pattern.node"),
    PATTERN_RELATIONSHIP(group = TopicTypeGroup.PATTERN, key = "$STREAMS_TOPIC_KEY.pattern.relationship"),
    CDC_SCHEMA(group = TopicTypeGroup.CDC, key = "$STREAMS_TOPIC_CDC_KEY.schema"),
    CUD(group = TopicTypeGroup.CUD, key = "$STREAMS_TOPIC_KEY.cud")
}

data class StreamsSinkEntity(val key: Any?, val value: Any?)

abstract class StreamsSinkService(private val strategyMap: Map<TopicType, Any>) {

    abstract fun getTopicType(topic: String): TopicType?
    abstract fun getCypherTemplate(topic: String): String?
    abstract fun write(query: String, events: Collection<Any>)

    private fun writeWithStrategy(data: Collection<StreamsSinkEntity>, strategy: IngestionStrategy) {
        strategy.mergeNodeEvents(data).forEach { write(it.query, it.events) }
        strategy.deleteNodeEvents(data).forEach { write(it.query, it.events) }

        strategy.mergeRelationshipEvents(data).forEach { write(it.query, it.events) }
        strategy.deleteRelationshipEvents(data).forEach { write(it.query, it.events) }
    }

    private fun writeWithCypherTemplate(topic: String, params: Collection<StreamsSinkEntity>) {
        val query = getCypherTemplate(topic) ?: return
        write(query, params.mapNotNull { it.value })
    }

    fun writeForTopic(topic: String, params: Collection<StreamsSinkEntity>) {
        val topicType = getTopicType(topic) ?: return
        when (topicType.group) {
            TopicTypeGroup.CYPHER -> writeWithCypherTemplate(topic, params)
            TopicTypeGroup.CDC, TopicTypeGroup.CUD -> writeWithStrategy(params, strategyMap.getValue(topicType) as IngestionStrategy)
            TopicTypeGroup.PATTERN -> {
                val strategyMap = strategyMap[topicType] as Map<String, IngestionStrategy>
                writeWithStrategy(params, strategyMap.getValue(topic))
            }
        }
    }
}