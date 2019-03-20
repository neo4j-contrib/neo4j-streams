package streams.service

import streams.service.sink.strategy.SchemaIngestionStrategy
import streams.service.sink.strategy.SourceIdIngestionStrategy
import streams.service.sink.strategy.SourceIdIngestionStrategyConfig
import kotlin.reflect.jvm.javaType

data class Topics(val cypherTopics: Map<String, String> = emptyMap(),
                  val cdcSourceIdTopics: Set<String> = emptySet(),
                  val cdcSchemaTopics: Set<String> = emptySet()) {

    fun allTopics(): List<String> = this.asMap()
            .map {
                if (it.key.group == TopicTypeGroup.CDC) {
                    (it.value as Set<String>).toList()
                } else {
                    (it.value as Map<String, Any>).keys.toList()
                }
            }
            .flatten()

    fun asMap(): Map<TopicType, Any> = mapOf(TopicType.CYPHER to cypherTopics,
            TopicType.CDC_SCHEMA to cdcSchemaTopics, TopicType.CDC_SOURCE_ID to cdcSourceIdTopics)

    companion object {
        fun from(config: Map<*, *>, prefix: String, toReplace: String = ""): Topics {
            val cypherTopicPrefix = TopicType.CYPHER.key.replace(prefix, toReplace)
            val sourceIdKey = TopicType.CDC_SOURCE_ID.key.replace(prefix, toReplace)
            val schemaKey = TopicType.CDC_SCHEMA.key.replace(prefix, toReplace)
            val cypherTopics = TopicUtils.filterByPrefix(config, cypherTopicPrefix)
            val cdcSourceIdTopics = TopicUtils.filterCDCTopics(config[sourceIdKey] as? String)
            val cdcSchemaTopics = TopicUtils.filterCDCTopics(config[schemaKey] as? String)
            return Topics(cypherTopics, cdcSourceIdTopics, cdcSchemaTopics)
        }
    }
}

object TopicUtils {

    @JvmStatic val CDC_TOPIC_SEPARATOR = ";"

    fun filterByPrefix(config: Map<*, *>, prefix: String): Map<String, String> {
        val fullPrefix = "$prefix."
        return config
                .filterKeys { it.toString().startsWith(fullPrefix) }
                .mapKeys { it.key.toString().replace(fullPrefix, "") }
                .mapValues { it.value.toString() }
    }

    fun filterCDCTopics(cdcMergeTopicsString: String?): Set<String> {
        return if (cdcMergeTopicsString.isNullOrBlank()) {
            emptySet()
        } else {
            cdcMergeTopicsString.split(CDC_TOPIC_SEPARATOR).toSet()
        }
    }

    inline fun <reified T: Throwable> validate(topics: Topics) {
        val crossDefinedTopics = (topics.cdcSourceIdTopics + topics.cdcSchemaTopics).intersect(topics.cypherTopics.keys)
        val exceptionStringConstructor = T::class.constructors
                .first { it.parameters.size == 1 && it.parameters[0].type.javaType == String::class.java }!!
        if (crossDefinedTopics.isNotEmpty()) {
            throw exceptionStringConstructor
                    .call("The following topics are cross defined between Cypher template configuration and CDC configuration: $crossDefinedTopics")
        }

        val cdcCrossDefinedTopics = topics.cdcSourceIdTopics.intersect(topics.cdcSchemaTopics)
        if (cdcCrossDefinedTopics.isNotEmpty()) {
            throw exceptionStringConstructor
                    .call("The following topics are cross defined between CDC Merge and CDC Schema configuration: $cdcCrossDefinedTopics")
        }
    }

    fun toStrategyMap(topics: Topics,
                      sourceIdStrategyConfig: SourceIdIngestionStrategyConfig): Map<TopicType, Any> = topics.asMap()
            .filterKeys { it != TopicType.CYPHER }
            .mapValues {
                when (it.key) {
                    TopicType.CDC_SOURCE_ID -> SourceIdIngestionStrategy(sourceIdStrategyConfig)
                    TopicType.CDC_SCHEMA -> SchemaIngestionStrategy()
                    else -> throw RuntimeException("Unsupported topic type ${it.key}")
                }
            }
}