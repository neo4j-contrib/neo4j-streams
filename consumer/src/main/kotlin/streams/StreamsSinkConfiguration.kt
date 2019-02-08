package streams

import org.apache.commons.lang3.StringUtils
import org.neo4j.kernel.configuration.Config


private object StreamsSinkConfigurationConstants {
    const val STREAMS_CONFIG_PREFIX: String = "streams."
    const val STREAMS_SINK_TOPIC_CYPHER_PREFIX: String = "sink.topic.cypher."
    const val STREAMS_SINK_TOPIC_CDC: String = "sink.topic.cdc.merge"
    const val ENABLED = "sink.enabled"
    const val PROCEDURES_ENABLED = "procedures.enabled"
}

data class StreamsSinkConfiguration(val enabled: Boolean = true,
                                    val proceduresEnabled: Boolean = true,
                                    val sinkPollingInterval: Long = 10000,
                                    val cypherTopics: Map<String, String> = emptyMap(),
                                    val cdcMergeTopics: Set<String> = emptySet()) {

    companion object {
        fun from(cfg: Config): StreamsSinkConfiguration {
            return from(cfg.raw)
        }

        fun from(cfg: Map<String, String>): StreamsSinkConfiguration {
            val default = StreamsSinkConfiguration()
            val config = cfg
                    .filterKeys { it.startsWith(StreamsSinkConfigurationConstants.STREAMS_CONFIG_PREFIX) }
                    .mapKeys { it.key.substring(StreamsSinkConfigurationConstants.STREAMS_CONFIG_PREFIX.length) }

            val cypherTopics = config
                    .filterKeys { it.startsWith(StreamsSinkConfigurationConstants.STREAMS_SINK_TOPIC_CYPHER_PREFIX) }
                    .mapKeys { it.key.replace(StreamsSinkConfigurationConstants.STREAMS_SINK_TOPIC_CYPHER_PREFIX, StringUtils.EMPTY) }
            val cdcTopicsString = config[StreamsSinkConfigurationConstants.STREAMS_SINK_TOPIC_CDC]
                    .orEmpty()
            val cdcTopics = if (cdcTopicsString == "") {
                emptySet()
            } else {
                cdcTopicsString.split(";").toSet()
            }
            val crossDefinedTopics = cdcTopics.intersect(cypherTopics.keys)
            if (crossDefinedTopics.isNotEmpty()) {
                throw RuntimeException("The following topics are cross defined between Cypher template configuration and CDC configuration: $crossDefinedTopics")
            }

            return default.copy(enabled = config.getOrDefault(StreamsSinkConfigurationConstants.ENABLED, default.enabled).toString().toBoolean(),
                    proceduresEnabled = config.getOrDefault(StreamsSinkConfigurationConstants.PROCEDURES_ENABLED, default.proceduresEnabled)
                            .toString().toBoolean(),
                    sinkPollingInterval = config.getOrDefault("sink.polling.interval", default.sinkPollingInterval).toString().toLong(),
                    cypherTopics = cypherTopics,
                    cdcMergeTopics = cdcTopics)
        }
    }

}