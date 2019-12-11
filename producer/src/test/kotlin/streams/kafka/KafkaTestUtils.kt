package streams.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer

object KafkaTestUtils {
    fun createConsumer(config: KafkaConfiguration): KafkaConsumer<String, ByteArray> {
        val props = config.asProperties()
        props["group.id"] = "neo4j"
        props["enable.auto.commit"] = "true"
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = ByteArrayDeserializer::class.java
        props["auto.offset.reset"] = "earliest"
        return KafkaConsumer(props)
    }

}