package kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.neo4j.kernel.configuration.Config
import java.util.*

data class KafkaConfiguration(val kafkaHosts: String = DEFAULTS.get("kafka.bootstrap.servers").toString(),
                              val acks: String = DEFAULTS.get("kafka.acks").toString(),
                              val retries: Int = DEFAULTS.get("kafka.retries").toString().toInt(),
                              val partitionSize: Int = DEFAULTS.get("kafka.partition.size").toString().toInt(),
                              val kafkaBatchSize: Int = DEFAULTS.get("kafka.batch.size").toString().toInt(),
                              val kafkaBufferSize: Int = DEFAULTS.get("kafka.buffer.memory").toString().toInt(),
                              val topic: String = DEFAULTS.get("kafka.topic").toString(),
                              val replication: Int = DEFAULTS.get("kafka.replication").toString().toInt(),
                              val groupId : String = DEFAULTS.get("kafka.group.id").toString()) {

    constructor(config: Map<String, Any>) : this(
            config.getOrDefault("kafka.bootstrap.servers", DEFAULTS.get("kafka.bootstrap.servers")).toString(),
            config.getOrDefault("kafka.acks", DEFAULTS.get("kafka.acks")).toString(),
            config.getOrDefault("kafka.retries", DEFAULTS.get("kafka.retries")).toString().toInt(),
            config.getOrDefault("kafka.partition.size", DEFAULTS.get("kafka.partition.size")).toString().toInt(),
            config.getOrDefault("kafka.batch.size", DEFAULTS.get("kafka.batch.size")).toString().toInt(),
            config.getOrDefault("kafka.buffer.memory", DEFAULTS.get("kafka.buffer.memory")).toString().toInt(),
            config.getOrDefault("kafka.topic", DEFAULTS.get("kafka.topic")).toString(),
            config.getOrDefault("kafka.replication", DEFAULTS.get("kafka.replication")).toString().toInt(),
            config.getOrDefault("kafka.group.id", DEFAULTS.get("kafka.group.id")).toString())

    companion object {
        val DEFAULTS = mapOf(
                "kafka.bootstrap.servers" to "localhost:9092",
                "kafka.acks" to "1",
                "kafka.retries" to 2,
                "kafka.batch.size" to 16384,
                "kafka.linger.ms" to 1,
                "kafka.partition.size" to 1,
                "kafka.buffer.memory" to 33554432,
                "kafka.replication" to 1,
                "kafka.group.id" to "neo4j",
                "kafka.topic" to "neo4j")
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
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.LongSerializer::class.java)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArraySerializer::class.java)
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.LongDeserializer::class.java)
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer::class.java)
        return props
    }
}
