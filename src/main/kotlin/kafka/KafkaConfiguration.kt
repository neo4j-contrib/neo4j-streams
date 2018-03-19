package kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import java.util.*

data class KafkaConfiguration(val zookeeperHosts: String = "localhost:2181", val kafkaHosts: String = "localhost:9092",
                                     val acks: String = "1", val partitionSize: Int = 1,
                                     val retries: Int = 2, val kafkaBatchSize: Int = 16384,
                                     val kafkaBufferSize: Int = 33554432, val reindexBatchSize: Int = 1000,
                                     val sessionTimeoutMs: Int = 15 * 1000, val connectTimeoutMs: Int = 10 * 1000,
                                     val topic: String = "neo4j", val replication: Int = 1, val groupId : String = "neo4j") {
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
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.LongSerializer::class.java)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArraySerializer::class.java)
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.LongDeserializer::class.java)
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer::class.java)
        return props
    }
}
