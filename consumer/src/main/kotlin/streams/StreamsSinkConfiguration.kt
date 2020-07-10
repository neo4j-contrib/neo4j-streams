package streams

import org.neo4j.kernel.configuration.Config
import streams.extensions.toPointCase
import streams.serialization.JSONUtils
import streams.service.TopicUtils
import streams.service.sink.strategy.SourceIdIngestionStrategyConfig
import streams.service.Topics
import java.util.Properties


object StreamsSinkConfigurationConstants {
    const val CHECK_APOC_TIMEOUT = "check.apoc.timeout"
    const val CHECK_APOC_INTERVAL = "check.apoc.interval"
    const val STREAMS_CONFIG_PREFIX = "streams."
    const val ENABLED = "sink.enabled"
    const val PROCEDURES_ENABLED = "procedures.enabled"
}

data class StreamsSinkConfiguration(val enabled: Boolean = false,
                                    val proceduresEnabled: Boolean = true,
                                    val topics: Topics = Topics(),
                                    val errorConfig: Map<String,Any?> = emptyMap(),
                                    val checkApocTimeout: Long = -1,
                                    val checkApocInterval: Long = 1000,
                                    val sourceIdStrategyConfig: SourceIdIngestionStrategyConfig = SourceIdIngestionStrategyConfig()) {

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
                    topics = topics,
                    errorConfig = errorHandler,
                    checkApocTimeout = config.getOrDefault(StreamsSinkConfigurationConstants.CHECK_APOC_TIMEOUT, default.checkApocTimeout)
                            .toString().toLong(),
                    checkApocInterval = config.getOrDefault(StreamsSinkConfigurationConstants.CHECK_APOC_INTERVAL, default.checkApocInterval)
                            .toString().toLong(),
                    sourceIdStrategyConfig = sourceIdStrategyConfig)
        }

    }

}