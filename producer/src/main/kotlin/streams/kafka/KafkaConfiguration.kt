package streams.kafka

import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import streams.extensions.getInt
import streams.extensions.toPointCase
import streams.serialization.JSONUtils
import streams.utils.ValidationUtils.validateConnection
import java.util.Properties
import java.util.concurrent.TimeUnit

private val configPrefix = "kafka."

data class KafkaConfiguration(val zookeeperConnect: String = "localhost:2181",
                              val bootstrapServers: String = "localhost:9092",
                              val acks: String = "1",
                              val retries: Int = 2,
                              val batchSize: Int = 16384,
                              val bufferMemory: Int = 33554432,
                              val reindexBatchSize: Int = 1000,
                              val sessionTimeoutMs: Int = 15 * 1000,
                              val connectionTimeoutMs: Int = 10 * 1000,
                              val replication: Int = 1,
                              val transactionalId: String = StringUtils.EMPTY,
                              val lingerMs: Int = 1,
                              val topicDiscoveryPollingInterval: Long = TimeUnit.MINUTES.toMillis(5),
                              val streamsLogCompactionStrategy: String = TopicConfig.CLEANUP_POLICY_DELETE,
                              val extraProperties: Map<String, String> = emptyMap()) {

    companion object {
        // Visible for testing
        fun create(cfg: Map<String, String>): KafkaConfiguration {
            val config = cfg.filterKeys { it.startsWith(configPrefix) }.mapKeys { it.key.substring(configPrefix.length) }

            val default = KafkaConfiguration()

            val keys = JSONUtils.asMap(default).keys.map { it.toPointCase() }
            val extraProperties = config.filterKeys { !keys.contains(it) }

            return default.copy(zookeeperConnect = config.getOrDefault("zookeeper.connect",default.zookeeperConnect),
                    bootstrapServers = config.getOrDefault("bootstrap.servers", default.bootstrapServers),
                    acks = config.getOrDefault("acks", default.acks),
                    retries = config.getInt("retries", default.retries),
                    batchSize = config.getInt("batch.size", default.batchSize),
                    bufferMemory = config.getInt("buffer.memory", default.bufferMemory),
                    reindexBatchSize = config.getInt("reindex.batch.size", default.reindexBatchSize),
                    sessionTimeoutMs = config.getInt("session.timeout.ms", default.sessionTimeoutMs),
                    connectionTimeoutMs = config.getInt("connection.timeout.ms", default.connectionTimeoutMs),
                    replication = config.getInt("replication", default.replication),
                    transactionalId = config.getOrDefault("transactional.id", default.transactionalId),
                    lingerMs = config.getInt("linger.ms", default.lingerMs),
                    topicDiscoveryPollingInterval = config.getOrDefault("topic.discovery.polling.interval",
                            default.topicDiscoveryPollingInterval).toString().toLong(),
                    streamsLogCompactionStrategy = config.getOrDefault("streams.log.compaction.strategy", default.streamsLogCompactionStrategy),
                    extraProperties = extraProperties // for what we don't provide a default configuration
            )
        }

        fun from(cfg: Map<String, String>): KafkaConfiguration {
            val kafkaCfg = create(cfg)
            validate(kafkaCfg, cfg)
            return kafkaCfg
        }

        private fun validate(config: KafkaConfiguration, rawConfig: Map<String, String>) {
            validateConnection(config.zookeeperConnect, "zookeeper.connect", false)
            validateConnection(config.bootstrapServers, CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, false)
        }

    }

    fun asProperties(): Properties {
        val props = Properties()
        val map = JSONUtils.asMap(this)
                .filter {
                    when (it.key) {
                        "transactionalId" -> it.value != StringUtils.EMPTY
                        "extraProperties" -> false
                        else -> true
                    }
                }
                .mapKeys { it.key.toPointCase() }
        props.putAll(map)
        props.putAll(extraProperties)
        props.putAll(addSerializers()) // Fixed serializers
        return props
    }

    private fun addSerializers() : Properties {
        val props = Properties()
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] =  ByteArraySerializer::class.java
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = ByteArraySerializer::class.java
        return props
    }

}