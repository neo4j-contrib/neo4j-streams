package streams

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

object KafkaTestUtils {
    fun <K, V> createConsumer(bootstrapServers: String,
                              schemaRegistryUrl: String? = null,
                              keyDeserializer: String = StringDeserializer::class.java.name,
                              valueDeserializer: String = ByteArrayDeserializer::class.java.name,
                              vararg topics: String = emptyArray()): KafkaConsumer<K, V> {
        val props = Properties()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        props["group.id"] = "neo4j" // UUID.randomUUID().toString()
        props["enable.auto.commit"] = "true"
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = keyDeserializer
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = valueDeserializer
        props["auto.offset.reset"] = "earliest"
        if (schemaRegistryUrl != null) {
            props["schema.registry.url"] = schemaRegistryUrl
        }
        val consumer = KafkaConsumer<K, V>(props)
        if (!topics.isNullOrEmpty()) {
            consumer.subscribe(topics.toList())
        }
        return consumer
    }

    fun <K, V> createProducer(bootstrapServers: String,
                              schemaRegistryUrl: String? = null,
                              keySerializer: String = StringSerializer::class.java.name,
                              valueSerializer: String = ByteArraySerializer::class.java.name): KafkaProducer<K, V> {
        val props = Properties()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = keySerializer
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = valueSerializer
        if (!schemaRegistryUrl.isNullOrBlank()) {
            props["schema.registry.url"] = schemaRegistryUrl
        }
        return KafkaProducer(props)
    }

}