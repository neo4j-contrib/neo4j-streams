package streams

import org.neo4j.kernel.configuration.Config
import streams.service.TopicType


private object StreamsSinkConfigurationConstants {
    const val STREAMS_CONFIG_PREFIX: String = "streams."
    const val ENABLED = "sink.enabled"
    const val PROCEDURES_ENABLED = "procedures.enabled"
}

private fun getCDCTopicFromConfig(type: TopicType, config: Map<String, String>): Set<String> {
    val cdcTopic = config[type.key.replace(StreamsSinkConfigurationConstants.STREAMS_CONFIG_PREFIX, "")].orEmpty()
    return if (cdcTopic == "") {
        emptySet()
    } else {
        cdcTopic.split(";").toSet()
    }
}

data class StreamsSinkConfiguration(val enabled: Boolean = true,
                                    val proceduresEnabled: Boolean = true,
                                    val sinkPollingInterval: Long = 10000,
                                    val cypherTopics: Map<String, String> = emptyMap(),
                                    val cdcTopics: Map<TopicType, Set<String>> = emptyMap()) {


    companion object {
        fun from(cfg: Config): StreamsSinkConfiguration {
            return from(cfg.raw)
        }

        fun from(cfg: Map<String, String>): StreamsSinkConfiguration {
            val default = StreamsSinkConfiguration()
            val config = cfg
                    .filterKeys { it.startsWith(StreamsSinkConfigurationConstants.STREAMS_CONFIG_PREFIX) }
                    .mapKeys { it.key.substring(StreamsSinkConfigurationConstants.STREAMS_CONFIG_PREFIX.length) }

            val cypherTopicPrefix = TopicType.CYPHER.key.replace(StreamsSinkConfigurationConstants.STREAMS_CONFIG_PREFIX, "")

            val cypherPrefix = "$cypherTopicPrefix."
            val cypherTopics = config
                    .filterKeys { it.startsWith(cypherPrefix) }
                    .mapKeys { it.key.replace(cypherPrefix, "") }
            val cdcMergeTopics = getCDCTopicFromConfig(TopicType.CDC_MERGE, config)
            val cdcSchemaTopics = getCDCTopicFromConfig(TopicType.CDC_SCHEMA, config)

            val crossDefinedTopics = (cdcMergeTopics + cdcSchemaTopics).intersect(cypherTopics.keys)
            if (crossDefinedTopics.isNotEmpty()) {
                throw RuntimeException("The following topics are cross defined between Cypher template configuration and CDC configuration: $crossDefinedTopics")
            }

            val cdcCrossDefinedTopics = cdcMergeTopics.intersect(cdcSchemaTopics)
            if (cdcCrossDefinedTopics.isNotEmpty()) {
                throw RuntimeException("The following topics are cross defined between CDC Merge and CDC Schema configuration: $cdcCrossDefinedTopics")
            }

            return default.copy(enabled = config.getOrDefault(StreamsSinkConfigurationConstants.ENABLED, default.enabled).toString().toBoolean(),
                    proceduresEnabled = config.getOrDefault(StreamsSinkConfigurationConstants.PROCEDURES_ENABLED, default.proceduresEnabled)
                            .toString().toBoolean(),
                    sinkPollingInterval = config.getOrDefault("sink.polling.interval", default.sinkPollingInterval).toString().toLong(),
                    cypherTopics = cypherTopics,
                    cdcTopics = mapOf(TopicType.CDC_MERGE to cdcMergeTopics, TopicType.CDC_SCHEMA to cdcSchemaTopics))
        }
    }

}