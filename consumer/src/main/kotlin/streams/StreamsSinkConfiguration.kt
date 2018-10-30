package streams

import org.apache.commons.lang3.StringUtils
import org.neo4j.kernel.configuration.Config


private const val streamsConfigPrefix = "streams."
private const val streamsSinkTopicPrefix = "sink.topic."

data class StreamsSinkConfiguration(val sinkPollingInterval: Long = Long.MAX_VALUE,
                                    val topics: Map<String, String> = emptyMap()) {

    companion object {
        fun from(cfg: Config): StreamsSinkConfiguration {
            val default = StreamsSinkConfiguration()
            val config = cfg.raw
                    .filterKeys { it.startsWith(streamsConfigPrefix) }
                    .mapKeys { it.key.substring(streamsConfigPrefix.length) }
            val topics = config
                    .filterKeys { it.startsWith(streamsSinkTopicPrefix) }
                    .mapKeys { it.key.replace(streamsSinkTopicPrefix, StringUtils.EMPTY) }
            return default.copy(sinkPollingInterval = config.getOrDefault("sink.polling.interval", default.sinkPollingInterval).toString().toLong(),
                    topics = topics)
        }
    }

}