package streams

import streams.config.StreamsConfig
import streams.service.TopicUtils
import streams.service.TopicValidationException
import streams.service.Topics
import streams.service.sink.strategy.SourceIdIngestionStrategyConfig

data class StreamsSinkConfiguration(val enabled: Boolean = StreamsConfig.SINK_ENABLED_VALUE,
                                    val proceduresEnabled: Boolean = StreamsConfig.PROCEDURES_ENABLED_VALUE,
                                    val topics: Topics = Topics(),
                                    val errorConfig: Map<String,Any?> = emptyMap(),
                                    val checkApocTimeout: Long = -1,
                                    val checkApocInterval: Long = 1000,
                                    val sourceIdStrategyConfig: SourceIdIngestionStrategyConfig = SourceIdIngestionStrategyConfig()) {

    companion object {
        fun from(cfg: StreamsConfig, dbName: String, invalidTopics: List<String> = emptyList()): StreamsSinkConfiguration {
            val default = StreamsSinkConfiguration()

            var topics = Topics.from(map = cfg.config, dbName = dbName, invalidTopics = invalidTopics)
            val isDefaultDb = cfg.isDefaultDb(dbName)
            if (isDefaultDb) {
                topics += Topics.from(map = cfg.config, invalidTopics = invalidTopics)
            }

            TopicUtils.validate<TopicValidationException>(topics)

            val sourceIdStrategyConfigPrefix = "streams.sink.topic.cdc.sourceId"
            val (sourceIdStrategyLabelNameKey, sourceIdStrategyIdNameKey) = if (isDefaultDb) {
                "labelName" to "idName"
            } else {
                "labelName.to.$dbName" to "idName.to.$dbName"
            }
            val defaultSourceIdStrategyConfig = SourceIdIngestionStrategyConfig()
            val sourceIdStrategyConfig = SourceIdIngestionStrategyConfig(
                    cfg.config.getOrDefault("$sourceIdStrategyConfigPrefix.$sourceIdStrategyLabelNameKey", defaultSourceIdStrategyConfig.labelName),
                    cfg.config.getOrDefault("$sourceIdStrategyConfigPrefix.$sourceIdStrategyIdNameKey", defaultSourceIdStrategyConfig.idName))

            val errorHandler = cfg.config
                    .filterKeys { it.startsWith("streams.sink.error") }
                    .mapKeys { it.key.substring("streams.sink.".length) }


            return default.copy(enabled = cfg.isSinkEnabled(dbName),
                    proceduresEnabled = cfg.hasProceduresEnabled(dbName),
                    topics = topics,
                    errorConfig = errorHandler,
                    checkApocTimeout = cfg.config.getOrDefault("streams.${StreamsConfig.CHECK_APOC_TIMEOUT}",
                            default.checkApocTimeout)
                            .toString()
                            .toLong(),
                    checkApocInterval = cfg.config.getOrDefault("streams.${StreamsConfig.CHECK_APOC_INTERVAL}",
                            default.checkApocInterval)
                            .toString()
                            .toLong(),
                    sourceIdStrategyConfig = sourceIdStrategyConfig)
        }

    }

}