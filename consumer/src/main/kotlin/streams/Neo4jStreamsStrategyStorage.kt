package streams

import streams.config.StreamsConfig
import streams.service.StreamsStrategyStorage
import streams.service.TopicType
import streams.service.sink.strategy.CUDIngestionStrategy
import streams.service.sink.strategy.CypherTemplateStrategy
import streams.service.sink.strategy.IngestionStrategy
import streams.service.sink.strategy.NodePatternConfiguration
import streams.service.sink.strategy.NodePatternIngestionStrategy
import streams.service.sink.strategy.RelationshipPatternConfiguration
import streams.service.sink.strategy.RelationshipPatternIngestionStrategy
import streams.service.sink.strategy.SchemaIngestionStrategy
import streams.service.sink.strategy.SourceIdIngestionStrategy

class Neo4jStreamsStrategyStorage(private val streamsTopicService: StreamsTopicService,
                                  private val streamsConfig: StreamsConfig,
                                  private val dbName: String): StreamsStrategyStorage() {

    override fun getTopicType(topic: String): TopicType? {
        return streamsTopicService.getTopicType(topic)
    }

    private fun <T> getTopicsByTopicType(topicType: TopicType): T = streamsTopicService.getByTopicType(topicType) as T

    override fun getStrategy(topic: String): IngestionStrategy = when (val topicType = getTopicType(topic)) {
        TopicType.CDC_SOURCE_ID -> {
            val strategyConfig = StreamsSinkConfiguration
                    .createSourceIdIngestionStrategyConfig(streamsConfig.config, dbName, streamsConfig.isDefaultDb(dbName))
            SourceIdIngestionStrategy(strategyConfig)
        }
        TopicType.CDC_SCHEMA -> SchemaIngestionStrategy()
        TopicType.CUD -> CUDIngestionStrategy()
        TopicType.PATTERN_NODE -> {
            val map = getTopicsByTopicType<Map<String, NodePatternConfiguration>>(topicType)
            NodePatternIngestionStrategy(map.getValue(topic))
        }
        TopicType.PATTERN_RELATIONSHIP -> {
            val map = getTopicsByTopicType<Map<String, RelationshipPatternConfiguration>>(topicType)
            RelationshipPatternIngestionStrategy(map.getValue(topic))
        }
        TopicType.CYPHER -> {
            CypherTemplateStrategy(streamsTopicService.getCypherTemplate(topic)!!)
        }
        else -> throw RuntimeException("Topic Type not Found")
    }

}