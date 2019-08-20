package streams

import org.neo4j.kernel.configuration.Config
import streams.service.TopicUtils
import streams.service.sink.strategy.SourceIdIngestionStrategyConfig
import streams.service.Topics


object StreamsSinkConfigurationConstants {
    const val STREAMS_CONFIG_PREFIX: String = "streams."
    const val ENABLED = "sink.enabled"
    const val PROCEDURES_ENABLED = "procedures.enabled"
}

data class StreamsSinkConfiguration(val enabled: Boolean = false,
                                    val proceduresEnabled: Boolean = true,
                                    val sinkPollingInterval: Long = 10000,
                                    val topics: Topics = Topics(),
                                    val errorConfig: Map<String,Any?> = emptyMap(),
                                    val sourceIdStrategyConfig: SourceIdIngestionStrategyConfig = SourceIdIngestionStrategyConfig()) {

    companion object {
        fun from(cfg: Config): StreamsSinkConfiguration {
            return from(cfg.raw)
        }

        fun from(cfg: Map<String, String>): StreamsSinkConfiguration {
            val default = StreamsSinkConfiguration()
            val config = cfg
                    .filterKeys { it.startsWith(StreamsSinkConfigurationConstants.STREAMS_CONFIG_PREFIX) }
                    .mapKeys { it.key.substring(StreamsSinkConfigurationConstants.STREAMS_CONFIG_PREFIX.length) }

            val topics = Topics.from(config, StreamsSinkConfigurationConstants.STREAMS_CONFIG_PREFIX)

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
                    sourceIdStrategyConfig = sourceIdStrategyConfig)
        }

    }

}