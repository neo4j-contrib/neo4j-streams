package streams.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.neo4j.kernel.configuration.Config
import streams.StreamsSinkConfiguration
import streams.extensions.toPointCase
import streams.serialization.JSONUtils
import java.util.*



private const val kafkaConfigPrefix = "kafka."

data class KafkaSinkConfiguration(val zookeeperConnect: String = "localhost:2181",
                                  val bootstrapServers: String = "localhost:9092",
                                  val groupId: String = "neo4j",
                                  val autoOffsetReset: String = "earliest",
                                  val streamsSinkConfiguration: StreamsSinkConfiguration = StreamsSinkConfiguration(),
                                  // val enableAutoCommit: String = true,
                                  val extraProperties: Map<String, String> = emptyMap()) {

    companion object {

        fun from(cfg: Config) : KafkaSinkConfiguration {
            return from(cfg.raw)
        }

        fun from(cfg: Map<String, String>) : KafkaSinkConfiguration {
            val config = cfg
                    .filterKeys { it.startsWith(kafkaConfigPrefix) }
                    .mapKeys { it.key.substring(kafkaConfigPrefix.length) }
            val default = KafkaSinkConfiguration()

            val keys = JSONUtils.asMap(default).keys.map { it.toPointCase() }
            val extraProperties = config.filterKeys { !keys.contains(it) }

            val streamsSinkConfiguration = StreamsSinkConfiguration.from(cfg)

            return default.copy(zookeeperConnect = config.getOrDefault("zookeeper.connect",default.zookeeperConnect),
                    bootstrapServers = config.getOrDefault("bootstrap.servers", default.bootstrapServers),
                    autoOffsetReset = config.getOrDefault("auto.offset.reset", default.autoOffsetReset),
                    groupId = config.getOrDefault("group.id", default.groupId),
                    // enableAutoCommit = config.getOrDefault("enable.auto.commit", default.enableAutoCommit).toBoolean(),
                    streamsSinkConfiguration = streamsSinkConfiguration,
                    extraProperties = extraProperties // for what we don't provide a default configuration
            )
        }
    }

    fun asProperties(): Properties {
        val props = Properties()
        val map = JSONUtils.asMap(this)
                .filterKeys { it != "extraProperties" && it != "streamsSinkConfiguration" }
                .mapKeys { it.key.toPointCase() }
        props.putAll(map)
        props.putAll(extraProperties)
        props.putAll(addDeserializers()) // Fixed deserializers
        props[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = true // Fixed autocommit
        return props
    }

    private fun addDeserializers() : Properties {
        val props = Properties()
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = ByteArrayDeserializer::class.java
        return props
    }
}