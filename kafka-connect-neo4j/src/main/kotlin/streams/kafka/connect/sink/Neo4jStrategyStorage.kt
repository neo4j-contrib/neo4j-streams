package streams.kafka.connect.sink

import streams.service.StreamsStrategyStorage
import streams.service.TopicType
import streams.service.sink.strategy.CUDIngestionStrategy
import streams.service.sink.strategy.CypherTemplateStrategy
import streams.service.sink.strategy.IngestionStrategy
import streams.service.sink.strategy.NodePatternIngestionStrategy
import streams.service.sink.strategy.RelationshipPatternIngestionStrategy
import streams.service.sink.strategy.SchemaIngestionStrategy
import streams.service.sink.strategy.SourceIdIngestionStrategy
import streams.service.sink.strategy.SourceIdIngestionStrategyConfig

class Neo4jStrategyStorage(val config: Neo4jSinkConnectorConfig): StreamsStrategyStorage() {
    private val topicConfigMap = config.topics.asMap()

    override fun getTopicType(topic: String): TopicType? = TopicType.values().firstOrNull { topicType ->
        when (val topicConfig = topicConfigMap.getOrDefault(topicType, emptyList<Any>())) {
            is Collection<*> -> topicConfig.contains(topic)
            is Map<*, *> -> topicConfig.containsKey(topic)
            is Pair<*, *> -> (topicConfig.first as Set<String>).contains(topic)
            else -> false
        }
    }

    override fun getStrategy(topic: String): IngestionStrategy = when (val topicType = getTopicType(topic)) {
        TopicType.CDC_SOURCE_ID -> config.strategyMap[topicType] as SourceIdIngestionStrategy
        TopicType.CDC_SCHEMA -> SchemaIngestionStrategy()
        TopicType.CUD -> CUDIngestionStrategy()
        TopicType.PATTERN_NODE -> NodePatternIngestionStrategy(config.topics.nodePatternTopics.getValue(topic))
        TopicType.PATTERN_RELATIONSHIP -> RelationshipPatternIngestionStrategy(config.topics.relPatternTopics.getValue(topic))
        TopicType.CYPHER -> CypherTemplateStrategy(config.topics.cypherTopics.getValue(topic))
        null -> throw RuntimeException("Topic Type not Found")
    }
}