package streams.kafka

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.neo4j.logging.Log
import streams.StreamsEventConsumer
import streams.extensions.offsetAndMetadata
import streams.extensions.toStreamsSinkEntity
import streams.extensions.topicPartition
import streams.service.StreamsSinkEntity
import streams.service.errors.*
import java.time.Duration

data class KafkaTopicConfig(val commit: Boolean, val topicPartitionsMap: Map<TopicPartition, Long>) {
    companion object {
        private fun toTopicPartitionMap(topicConfig: Map<String,
                List<Map<String, Any>>>): Map<TopicPartition, Long> = topicConfig
                .flatMap { topicConfigEntry ->
                    topicConfigEntry.value.map {
                        val partition = it.getValue("partition").toString().toInt()
                        val offset = it.getValue("offset").toString().toLong()
                        TopicPartition(topicConfigEntry.key, partition) to offset
                    }
                }
                .toMap()

        fun fromMap(map: Map<String, Any>): KafkaTopicConfig {
            val commit = map.getOrDefault("commit", true).toString().toBoolean()
            val topicPartitionsMap = toTopicPartitionMap(map
                    .getOrDefault("partitions", emptyMap<String, List<Map<String, Any>>>()) as Map<String, List<Map<String, Any>>>)
            return KafkaTopicConfig(commit = commit, topicPartitionsMap = topicPartitionsMap)
        }
    }
}

open class KafkaAutoCommitEventConsumer(private val config: KafkaSinkConfiguration,
                                        private val log: Log,
                                        private val errorService: ErrorService): StreamsEventConsumer(log, errorService) {

    override fun invalidTopics(): List<String> = config.streamsSinkConfiguration.topics.invalid

    private var isSeekSet = false

    val consumer: KafkaConsumer<*, *> = when {
        config.keyDeserializer == ByteArrayDeserializer::class.java.name && config.valueDeserializer == ByteArrayDeserializer::class.java.name -> KafkaConsumer<ByteArray, ByteArray>(config.asProperties())
        config.keyDeserializer == ByteArrayDeserializer::class.java.name && config.valueDeserializer == KafkaAvroDeserializer::class.java.name -> KafkaConsumer<ByteArray, GenericRecord>(config.asProperties())
        config.keyDeserializer == KafkaAvroDeserializer::class.java.name && config.valueDeserializer == KafkaAvroDeserializer::class.java.name -> KafkaConsumer<GenericRecord, GenericRecord>(config.asProperties())
        config.keyDeserializer == KafkaAvroDeserializer::class.java.name && config.valueDeserializer == ByteArrayDeserializer::class.java.name -> KafkaConsumer<GenericRecord, ByteArray>(config.asProperties())
        else -> throw RuntimeException("Invalid config")
    }

    lateinit var topics: Set<String>

    override fun withTopics(topics: Set<String>): StreamsEventConsumer {
        this.topics = topics
        return this
    }

    override fun start() {
        if (topics.isEmpty()) {
            log.info("No topics specified Kafka Consumer will not started")
            return
        }
        this.consumer.subscribe(topics)
    }

    override fun stop() {
        consumer.close()
        // doesn't make sense to close it if you don't own it
        errorService.close()
    }

    fun readSimple(action: (String, List<StreamsSinkEntity>) -> Unit): Map<TopicPartition, OffsetAndMetadata> {
        val records = consumer.poll(Duration.ZERO)
        return when (records.isEmpty) {
            true -> emptyMap()
            else -> this.topics
                    .filter { topic -> records.records(topic).iterator().hasNext() }
                    .flatMap { topic -> records.records(topic).map { it.topicPartition() to it } }
                    .groupBy({ it.first }, { it.second })
                    .mapValues {
                        executeAction(action, it.key.topic(), it.value)
                        it.value.last().offsetAndMetadata()
                    }
        }
    }

    private fun executeAction(action: (String, List<StreamsSinkEntity>) -> Unit, topic: String, topicRecords: Iterable<ConsumerRecord<out Any, out Any>>) {
        try {
            action(topic, convert(topicRecords))
        } catch (e: Exception) {
            errorService.report(topicRecords.map { ErrorData.from(it, e,this::class.java ) })
        }
    }

    private fun convert(topicRecords: Iterable<ConsumerRecord<out Any, out Any>>) = topicRecords
            .map {
                try {
                    "ok" to it.toStreamsSinkEntity()
                } catch (e: Exception) {
                    "error" to ErrorData.from(it, e, this::class.java)
                }
            }
            .groupBy({ it.first }, { it.second })
            .let {
                it.get("error")?.let {
                    errorService.report(it as List<ErrorData>)
                }
                it.getOrDefault("ok", emptyList()) as List<StreamsSinkEntity>
            }

    fun readFromPartition(kafkaTopicConfig: KafkaTopicConfig,
                          action: (String, List<StreamsSinkEntity>) -> Unit): Map<TopicPartition, OffsetAndMetadata> {
        setSeek(kafkaTopicConfig.topicPartitionsMap)
        val records = consumer.poll(Duration.ZERO)
        return when (records.isEmpty) {
            true -> emptyMap()
            else -> kafkaTopicConfig.topicPartitionsMap
                    .mapValues { records.records(it.key) }
                    .filterValues { it.isNotEmpty() }
                    .mapValues { (topic, topicRecords) ->
                        executeAction(action, topic.topic(), topicRecords)
                        topicRecords.last().offsetAndMetadata()
                    }
        }
    }

    override fun read(action: (String, List<StreamsSinkEntity>) -> Unit) {
        readSimple(action)
    }

    override fun read(topicConfig: Map<String, Any>, action: (String, List<StreamsSinkEntity>) -> Unit) {
        val kafkaTopicConfig = KafkaTopicConfig.fromMap(topicConfig)
        if (kafkaTopicConfig.topicPartitionsMap.isEmpty()) {
            readSimple(action)
        } else {
            readFromPartition(kafkaTopicConfig, action)
        }
    }

    private fun setSeek(topicPartitionsMap: Map<TopicPartition, Long>) {
        if (isSeekSet) {
            return
        }
        isSeekSet = true
        consumer.poll(0) // dummy call see: https://stackoverflow.com/questions/41008610/kafkaconsumer-0-10-java-api-error-message-no-current-assignment-for-partition
        topicPartitionsMap.forEach {
            when (it.value) {
                -1L -> consumer.seekToBeginning(listOf(it.key))
                -2L -> consumer.seekToEnd(listOf(it.key))
                else -> consumer.seek(it.key, it.value)
            }
        }
    }
}

