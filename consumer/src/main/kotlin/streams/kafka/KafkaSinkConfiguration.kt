package streams.kafka

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.neo4j.kernel.configuration.Config
import streams.StreamsSinkConfiguration
import streams.extensions.toPointCase
import streams.serialization.JSONUtils
import streams.utils.KafkaValidationUtils.getInvalidTopics
import streams.utils.ValidationUtils.validateConnection
import java.util.*


private const val kafkaConfigPrefix = "kafka."

private val SUPPORTED_DESERIALIZER = listOf(ByteArrayDeserializer::class.java.name, KafkaAvroDeserializer::class.java.name)

private fun validateDeserializers(config: KafkaSinkConfiguration) {
    val key = if (!SUPPORTED_DESERIALIZER.contains(config.keyDeserializer)) {
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG
    } else if (!SUPPORTED_DESERIALIZER.contains(config.valueDeserializer)) {
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
    } else {
        ""
    }
    if (key.isNotBlank()) {
        throw RuntimeException("The property `kafka.$key` contains an invalid deserializer. Supported deserializers are $SUPPORTED_DESERIALIZER")
    }
}

data class KafkaSinkConfiguration(val zookeeperConnect: String = "localhost:2181",
                                  val bootstrapServers: String = "localhost:9092",
                                  val keyDeserializer: String = "org.apache.kafka.common.serialization.ByteArrayDeserializer",
                                  val valueDeserializer: String = "org.apache.kafka.common.serialization.ByteArrayDeserializer",
                                  val groupId: String = "neo4j",
                                  val autoOffsetReset: String = "earliest",
                                  val streamsSinkConfiguration: StreamsSinkConfiguration = StreamsSinkConfiguration(),
                                  val enableAutoCommit: Boolean = true,
                                  val streamsAsyncCommit: Boolean = false,
                                  val extraProperties: Map<String, String> = emptyMap()) {

    companion object {

        fun from(cfg: Config): KafkaSinkConfiguration {
            return from(cfg.raw)
        }

        fun from(cfg: Map<String, String>): KafkaSinkConfiguration {
            val kafkaCfg = create(cfg)
            validate(kafkaCfg)
            val invalidTopics = getInvalidTopics(kafkaCfg.asProperties(), kafkaCfg.streamsSinkConfiguration.topics.allTopics())
            return if (invalidTopics.isNotEmpty()) {
                kafkaCfg.copy(streamsSinkConfiguration = StreamsSinkConfiguration.from(cfg, invalidTopics))
            } else {
                kafkaCfg
            }
        }

        // Visible for testing
        fun create(cfg: Map<String, String>): KafkaSinkConfiguration {
            val config = cfg
                    .filterKeys { it.startsWith(kafkaConfigPrefix) }
                    .mapKeys { it.key.substring(kafkaConfigPrefix.length) }
            val default = KafkaSinkConfiguration()

            val keys = JSONUtils.asMap(default).keys.map { it.toPointCase() }
            val extraProperties = config.filterKeys { !keys.contains(it) }

            val streamsSinkConfiguration = StreamsSinkConfiguration.from(cfg)
            return default.copy(zookeeperConnect = config.getOrDefault("zookeeper.connect",default.zookeeperConnect),
                    keyDeserializer = config.getOrDefault(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, default.keyDeserializer),
                    valueDeserializer = config.getOrDefault(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, default.valueDeserializer),
                    bootstrapServers = config.getOrDefault(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, default.bootstrapServers),
                    autoOffsetReset = config.getOrDefault(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, default.autoOffsetReset),
                    groupId = config.getOrDefault(ConsumerConfig.GROUP_ID_CONFIG, default.groupId),
                    enableAutoCommit = config.getOrDefault(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, default.enableAutoCommit).toString().toBoolean(),
                    streamsAsyncCommit = config.getOrDefault("streams.async.commit", default.streamsAsyncCommit).toString().toBoolean(),
                    streamsSinkConfiguration = streamsSinkConfiguration,
                    extraProperties = extraProperties // for what we don't provide a default configuration
            )
        }

        private fun validate(config: KafkaSinkConfiguration) {
            validateConnection(config.zookeeperConnect, "zookeeper.connect", false)
            validateConnection(config.bootstrapServers, CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, false)
            val schemaRegistryUrlKey = "schema.registry.url"
            if (config.extraProperties.containsKey(schemaRegistryUrlKey)) {
                val schemaRegistryUrl = config.extraProperties.getOrDefault(schemaRegistryUrlKey, "")
                validateConnection(schemaRegistryUrl, schemaRegistryUrlKey, false)
            }
            validateDeserializers(config)
        }
    }

    fun asProperties(): Properties {
        val props = Properties()
        val map = JSONUtils.asMap(this)
                .filterKeys { it != "extraProperties" && it != "streamsSinkConfiguration" }
                .mapKeys { it.key.toPointCase() }
        props.putAll(map)
        props.putAll(extraProperties)
        return props
    }
}