package kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.LongDeserializer
import org.apache.kafka.common.serialization.LongSerializer
import java.util.*

fun Map<String,String>.getInt(name:String, defaultValue: Int) = this.get(name)?.toInt() ?: defaultValue

data class KafkaConfiguration(val zookeeperHosts: String = "localhost:2181",
                              val kafkaHosts: String = "localhost:9092",
                              val acks: String = "1",
                              val partitionSize: Int = 1,
                              val retries: Int = 2,
                              val kafkaBatchSize: Int = 16384,
                              val kafkaBufferSize: Int = 33554432,
                              val reindexBatchSize: Int = 1000,
                              val sessionTimeoutMs: Int = 15 * 1000,
                              val connectTimeoutMs: Int = 10 * 1000,
                              val replication: Int = 1,
                              val groupId: String = "neo4j",
                              val topics: List<String> = listOf("neo4j"),
                              val patterns: List<NodePattern> = listOf(NodePattern(topic=topics.first()))) {

    companion object {
        val commaRegexp = "\\s,\\s".toRegex()
        fun from(config: Map<String,String>) : KafkaConfiguration {
            val default = KafkaConfiguration()
            val topics = config.get("topics")?.split(commaRegexp) ?: default.topics
            return default.copy(zookeeperHosts = config.getOrDefault("zookeeper.connect",default.zookeeperHosts),
                    kafkaHosts = config.getOrDefault("bootstrap.servers", default.kafkaHosts),
                    acks = config.getOrDefault("acks", default.acks),
                    partitionSize = config.getInt("num.partitions", default.partitionSize),
                    retries = config.getInt("retries", default.retries),
                    kafkaBatchSize = config.getInt("batch.size", default.kafkaBatchSize),
                    kafkaBufferSize = config.getInt("buffer.memory", default.kafkaBufferSize),
                    reindexBatchSize = config.getInt("reindex.batch.size", default.reindexBatchSize),
                    sessionTimeoutMs = config.getInt("session.timeout.ms", default.sessionTimeoutMs), 
                    connectTimeoutMs = config.getInt("connection.timeout.ms", default.connectTimeoutMs), 
                    replication = config.getInt("replication", default.replication),
                    groupId = config.getOrDefault("group.id", default.groupId),
                    topics = topics,
                    patterns = NodePattern.parse(config.getOrDefault("patterns","neo4j:*"), topics = topics)
            )
        }
    }

    fun asProperties(): Properties {
        val props = Properties()
        props.put("bootstrap.servers", kafkaHosts)
        props.put("acks", acks)
        props.put("retries", retries)
        props.put("batch.size", kafkaBatchSize)
        props.put("linger.ms", 1)
        props.put("buffer.memory", kafkaBufferSize)
        props.put("group.id", groupId)
        /*
        props.put("enable.auto.commit", "true")
        props.put("auto.commit.interval.ms", "1000")
         */
        props.putAll(addSerializers())
        return props
    }

    private fun addSerializers() : Properties {
        val props = Properties()
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer::class.java)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer::class.java)
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer::class.java)
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer::class.java)
        return props
    }
}
