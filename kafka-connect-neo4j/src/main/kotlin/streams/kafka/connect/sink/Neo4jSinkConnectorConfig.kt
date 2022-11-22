package streams.kafka.connect.sink

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.sink.SinkTask
import streams.kafka.connect.common.ConfigGroup
import streams.kafka.connect.common.ConnectorType
import streams.kafka.connect.common.Neo4jConnectorConfig
import streams.kafka.connect.utils.PropertiesUtil
import streams.service.TopicType
import streams.service.TopicUtils
import streams.service.Topics
import streams.service.sink.strategy.SourceIdIngestionStrategyConfig

enum class AuthenticationType {
    NONE, BASIC, KERBEROS
}

class Neo4jSinkConnectorConfig(originals: Map<*, *>): Neo4jConnectorConfig(config(), originals, ConnectorType.SINK) {

    val parallelBatches: Boolean

    val topics: Topics

    val strategyMap: Map<TopicType, Any>

    val kafkaBrokerProperties: Map<String, Any?>

    init {
        topics = Topics.from(originals as Map<String, Any?>, "streams.sink." to "neo4j.")
        strategyMap = TopicUtils.toStrategyMap(topics)

        parallelBatches = getBoolean(BATCH_PARALLELIZE)
        val kafkaPrefix = "kafka."
        kafkaBrokerProperties = originals
                .filterKeys { it.startsWith(kafkaPrefix) }
                .mapKeys { it.key.substring(kafkaPrefix.length) }
        validateAllTopics(originals)
    }

    private fun validateAllTopics(originals: Map<*, *>) {
        TopicUtils.validate<ConfigException>(this.topics)
        val topics = if (originals.containsKey(SinkTask.TOPICS_CONFIG)) {
            originals[SinkTask.TOPICS_CONFIG].toString()
                    .split(",")
                    .map { it.trim() }
                    .sorted()
        } else { // TODO manage regexp
            emptyList()
        }
        val allTopics = this.topics
                .allTopics()
                .sorted()
        if (topics != allTopics) {
            throw ConfigException("There is a mismatch between topics defined into the property `${SinkTask.TOPICS_CONFIG}` ($topics) and configured topics ($allTopics)")
        }
    }

    companion object {

        const val BATCH_PARALLELIZE = "neo4j.batch.parallelize"

        const val TOPIC_CYPHER_PREFIX = "neo4j.topic.cypher."
        const val TOPIC_CDC_SOURCE_ID = "neo4j.topic.cdc.sourceId"
        const val TOPIC_CDC_SOURCE_ID_LABEL_NAME = "neo4j.topic.cdc.sourceId.labelName"
        const val TOPIC_CDC_SOURCE_ID_ID_NAME = "neo4j.topic.cdc.sourceId.idName"
        const val TOPIC_PATTERN_NODE_PREFIX = "neo4j.topic.pattern.node."
        const val TOPIC_PATTERN_RELATIONSHIP_PREFIX = "neo4j.topic.pattern.relationship."
        const val TOPIC_PATTERN_MERGE_NODE_PROPERTIES_ENABLED = "neo4j.topic.pattern.merge.node.properties.enabled"
        const val TOPIC_PATTERN_MERGE_RELATIONSHIP_PROPERTIES_ENABLED = "neo4j.topic.pattern.merge.relationship.properties.enabled"
        const val TOPIC_CDC_SCHEMA = "neo4j.topic.cdc.schema"
        const val TOPIC_CUD = "neo4j.topic.cud"


        const val DEFAULT_BATCH_PARALLELIZE = true
        const val DEFAULT_TOPIC_PATTERN_MERGE_NODE_PROPERTIES_ENABLED = false
        const val DEFAULT_TOPIC_PATTERN_MERGE_RELATIONSHIP_PROPERTIES_ENABLED = false



        private val sourceIdIngestionStrategyConfig = SourceIdIngestionStrategyConfig()

        fun config(): ConfigDef = Neo4jConnectorConfig.config()
                    .define(ConfigKeyBuilder.of(TOPIC_CDC_SOURCE_ID, ConfigDef.Type.STRING)
                            .documentation(PropertiesUtil.getProperty(TOPIC_CDC_SOURCE_ID)).importance(ConfigDef.Importance.HIGH)
                            .defaultValue("").group(ConfigGroup.TOPIC_CYPHER_MAPPING)
                            .build())
                    .define(ConfigKeyBuilder.of(TOPIC_CDC_SOURCE_ID_LABEL_NAME, ConfigDef.Type.STRING)
                            .documentation(PropertiesUtil.getProperty(TOPIC_CDC_SOURCE_ID_LABEL_NAME)).importance(ConfigDef.Importance.HIGH)
                            .defaultValue(sourceIdIngestionStrategyConfig.labelName).group(ConfigGroup.TOPIC_CYPHER_MAPPING)
                            .build())
                    .define(ConfigKeyBuilder.of(TOPIC_CDC_SOURCE_ID_ID_NAME, ConfigDef.Type.STRING)
                            .documentation(PropertiesUtil.getProperty(TOPIC_CDC_SOURCE_ID_ID_NAME)).importance(ConfigDef.Importance.HIGH)
                            .defaultValue(sourceIdIngestionStrategyConfig.idName).group(ConfigGroup.TOPIC_CYPHER_MAPPING)
                            .build())
                    .define(ConfigKeyBuilder.of(TOPIC_CDC_SCHEMA, ConfigDef.Type.STRING)
                            .documentation(PropertiesUtil.getProperty(TOPIC_CDC_SCHEMA)).importance(ConfigDef.Importance.HIGH)
                            .defaultValue("").group(ConfigGroup.TOPIC_CYPHER_MAPPING)
                            .build())
                    .define(ConfigKeyBuilder.of(BATCH_PARALLELIZE, ConfigDef.Type.BOOLEAN)
                            .documentation(PropertiesUtil.getProperty(BATCH_PARALLELIZE)).importance(ConfigDef.Importance.MEDIUM)
                            .defaultValue(DEFAULT_BATCH_PARALLELIZE).group(ConfigGroup.BATCH)
                            .build())
                    .define(ConfigKeyBuilder.of(TOPIC_CUD, ConfigDef.Type.STRING)
                            .documentation(PropertiesUtil.getProperty(TOPIC_CUD)).importance(ConfigDef.Importance.HIGH)
                            .defaultValue("").group(ConfigGroup.TOPIC_CYPHER_MAPPING)
                            .build())
                    .define(ConfigKeyBuilder.of(TOPIC_PATTERN_MERGE_NODE_PROPERTIES_ENABLED, ConfigDef.Type.BOOLEAN)
                        .documentation(PropertiesUtil.getProperty(TOPIC_PATTERN_MERGE_NODE_PROPERTIES_ENABLED)).importance(ConfigDef.Importance.MEDIUM)
                        .defaultValue(DEFAULT_TOPIC_PATTERN_MERGE_NODE_PROPERTIES_ENABLED).group(ConfigGroup.TOPIC_CYPHER_MAPPING)
                        .build())
                    .define(ConfigKeyBuilder.of(TOPIC_PATTERN_MERGE_RELATIONSHIP_PROPERTIES_ENABLED, ConfigDef.Type.BOOLEAN)
                        .documentation(PropertiesUtil.getProperty(TOPIC_PATTERN_MERGE_RELATIONSHIP_PROPERTIES_ENABLED)).importance(ConfigDef.Importance.MEDIUM)
                        .defaultValue(DEFAULT_TOPIC_PATTERN_MERGE_RELATIONSHIP_PROPERTIES_ENABLED).group(ConfigGroup.TOPIC_CYPHER_MAPPING)
                        .build())
    }
}