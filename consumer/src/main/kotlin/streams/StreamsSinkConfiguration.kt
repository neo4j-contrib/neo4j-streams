package streams

import org.neo4j.kernel.configuration.Config
import streams.service.TopicUtils
import streams.service.sink.strategy.SourceIdIngestionStrategyConfig
import streams.service.Topics


object StreamsSinkConfigurationConstants {
    const val STREAMS_CONFIG_PREFIX: String = "streams."
    const val ENABLED = "sink.enabled"
    const val PROCEDURES_ENABLED = "procedures.enabled"
    const val EVENT_PREFIX_TIMESTAMP = "event.timestamp"
    const val EVENT_PREFIX_TIMESTAMP_DEFAULT = "__timestamp__"
    const val EVENT_PREFIX_HEADERS = "event.headers"
    const val EVENT_PREFIX_HEADERS_DEFAULT = "__headers__"
    const val EVENT_PREFIX_KEY = "event.key"
    const val EVENT_PREFIX_KEY_DEFAULT = "__key__"
}

data class StreamsSinkConfiguration(val enabled: Boolean = false,
                                    val proceduresEnabled: Boolean = true,
                                    val sinkPollingInterval: Long = 10000,
                                    val topics: Topics = Topics(),
                                    val errorConfig: Map<String,Any?> = emptyMap(),
                                    val sourceIdStrategyConfig: SourceIdIngestionStrategyConfig = SourceIdIngestionStrategyConfig(),
                                    val eventPrefixTimestamp: String = StreamsSinkConfigurationConstants.EVENT_PREFIX_TIMESTAMP_DEFAULT,
                                    val eventPrefixHeaders: String = StreamsSinkConfigurationConstants.EVENT_PREFIX_HEADERS_DEFAULT,
                                    val eventPrefixKey: String = StreamsSinkConfigurationConstants.EVENT_PREFIX_KEY_DEFAULT) {

    companion object {
        fun from(cfg: Config): StreamsSinkConfiguration {
            return from(cfg.raw)
        }

        fun from(cfg: Map<String, String>, invalidTopics: List<String> = emptyList()): StreamsSinkConfiguration {
            val default = StreamsSinkConfiguration()
            val config = cfg
                    .filterKeys { it.startsWith(StreamsSinkConfigurationConstants.STREAMS_CONFIG_PREFIX) }
                    .mapKeys { it.key.substring(StreamsSinkConfigurationConstants.STREAMS_CONFIG_PREFIX.length) }

            val topics = Topics.from(config = config,
                    prefix = StreamsSinkConfigurationConstants.STREAMS_CONFIG_PREFIX,
                    invalidTopics = invalidTopics)

            TopicUtils.validate<RuntimeException>(topics)

            val defaultSourceIdStrategyConfig = SourceIdIngestionStrategyConfig()

            val sourceIdStrategyConfig = SourceIdIngestionStrategyConfig(
                    config.getOrDefault("sink.topic.cdc.sourceId.labelName", defaultSourceIdStrategyConfig.labelName),
                    config.getOrDefault("sink.topic.cdc.sourceId.idName", defaultSourceIdStrategyConfig.idName))

            val errorHandler = config.filterKeys { it.startsWith("sink.error") }.mapKeys { it.key.substring("sink.".length) }


            return default.copy(enabled = config.getOrDefault(StreamsSinkConfigurationConstants.ENABLED, default.enabled).toString().toBoolean(),
                    proceduresEnabled = config.getOrDefault(StreamsSinkConfigurationConstants.PROCEDURES_ENABLED, default.proceduresEnabled)
                            .toString().toBoolean(),
                    sinkPollingInterval = config.getOrDefault("sink.polling.interval", default.sinkPollingInterval).toString().toLong(),
                    topics = topics,
                    errorConfig = errorHandler,
                    sourceIdStrategyConfig = sourceIdStrategyConfig,
                    eventPrefixTimestamp = config.getOrDefault(StreamsSinkConfigurationConstants.EVENT_PREFIX_TIMESTAMP, default.eventPrefixTimestamp),
                    eventPrefixHeaders = config.getOrDefault(StreamsSinkConfigurationConstants.EVENT_PREFIX_HEADERS, default.eventPrefixHeaders),
                    eventPrefixKey = config.getOrDefault(StreamsSinkConfigurationConstants.EVENT_PREFIX_KEY, default.eventPrefixKey)
            )
        }

    }

}