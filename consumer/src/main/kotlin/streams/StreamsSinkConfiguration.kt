package streams

import org.neo4j.kernel.configuration.Config
import streams.extensions.toPointCase
import streams.procedures.StreamsSinkProcedures
import streams.serialization.JSONUtils
import streams.service.TopicUtils
import streams.service.sink.strategy.SourceIdIngestionStrategyConfig
import streams.service.Topics
import java.util.concurrent.TimeUnit


object StreamsSinkConfigurationConstants {
    const val CHECK_APOC_TIMEOUT = "check.apoc.timeout"
    const val CHECK_APOC_INTERVAL = "check.apoc.interval"
    const val STREAMS_CONFIG_PREFIX = "streams."
    const val ENABLED = "sink.enabled"
    const val PROCEDURES_ENABLED = "procedures.enabled"
    const val CLUSTER_ONLY = "cluster.only"
    const val CHECK_WRITEABLE_INSTANCE_INTERVAL = "check.writeable.instance.interval"
    const val POLL_INTERVAL = "sink.poll.interval"
}

data class StreamsSinkConfiguration(val enabled: Boolean = false,
                                    val proceduresEnabled: Boolean = true,
                                    val topics: Topics = Topics(),
                                    val errorConfig: Map<String,Any?> = emptyMap(),
                                    val checkApocTimeout: Long = -1,
                                    val checkApocInterval: Long = 1000,
                                    val clusterOnly: Boolean = false,
                                    val checkWriteableInstanceInterval: Long = TimeUnit.MINUTES.toMillis(3),
                                    val pollInterval: Long = TimeUnit.SECONDS.toMillis(0),
                                    val sourceIdStrategyConfig: SourceIdIngestionStrategyConfig = SourceIdIngestionStrategyConfig()) {

    fun asMap(): Map<String, Any?> {
        val configMap = JSONUtils.asMap(this)
                .filterKeys { it != "topics" && it != "enabled" && it != "proceduresEnabled" && !it.startsWith("check") }
                .mapKeys { it.key.toPointCase() }
                .mapKeys {
                    when (it.key) {
                        "error.config" -> "streams.sink.errors"
                        "procedures.enabled" -> "streams.${it.key}"
                        "cluster.only" -> "streams.${it.key}"
                        else -> if (it.key.startsWith("streams.sink")) it.key else "streams.sink.${it.key}"
                    }
                }
        val topicMap = this.topics.asMap()
                .mapKeys { it.key.key }
        val invalidTopics = mapOf("invalid_topics" to this.topics.invalid)
        return (configMap + topicMap + invalidTopics)
    }

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
                    checkWriteableInstanceInterval = config.getOrDefault(StreamsSinkConfigurationConstants.CHECK_WRITEABLE_INSTANCE_INTERVAL, default.checkWriteableInstanceInterval)
                            .toString().toLong(),
                    pollInterval = config.getOrDefault(StreamsSinkConfigurationConstants.POLL_INTERVAL, default.pollInterval)
                            .toString().toLong(),
                    clusterOnly = config.getOrDefault(StreamsSinkConfigurationConstants.CLUSTER_ONLY, default.clusterOnly)
                            .toString().toBoolean(),
                    sourceIdStrategyConfig = sourceIdStrategyConfig)
        }

    }

}