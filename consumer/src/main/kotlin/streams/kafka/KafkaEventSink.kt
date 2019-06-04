package streams.kafka

import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.*
import org.apache.kafka.common.TopicPartition
import org.neo4j.kernel.configuration.Config
import org.neo4j.logging.Log
import streams.*
import streams.kafka.KafkaTopicConfig.Companion.toTopicPartitionMap
import streams.serialization.JSONUtils
import streams.utils.StreamsUtils
import java.util.concurrent.ConcurrentHashMap


class KafkaEventSink(private val config: Config,
                     private val queryExecution: StreamsEventSinkQueryExecution,
                     private val streamsTopicService: StreamsTopicService,
                     private val log: Log): StreamsEventSink(config, queryExecution, streamsTopicService, log) {

    private lateinit var eventConsumer: StreamsEventConsumer
    private lateinit var job: Job

    private val streamsConfigMap = config.raw.filterKeys {
        it.startsWith("kafka.") || (it.startsWith("streams.") && !it.startsWith("streams.sink.topic.cypher."))
    }.toMap()

    private val mappingKeys = mapOf(
            "zookeeper" to "kafka.zookeeper.connect",
            "broker" to "kafka.${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}",
            "from" to "kafka.${ConsumerConfig.AUTO_OFFSET_RESET_CONFIG}",
            "autoCommit" to "kafka.${ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG}",
            "groupId" to "kafka.${ConsumerConfig.GROUP_ID_CONFIG}")

    override fun getEventConsumerFactory(): StreamsEventConsumerFactory {
        return object: StreamsEventConsumerFactory() {
            override fun createStreamsEventConsumer(config: Map<String, String>, log: Log): StreamsEventConsumer {
                val kafkaConfig = KafkaSinkConfiguration.from(config)
                return if (kafkaConfig.enableAutoCommit) {
                    KafkaAutoCommitEventConsumer(kafkaConfig, log)
                } else {
                    KafkaManualCommitEventConsumer(kafkaConfig, log)
                }
            }
        }
    }

    override fun start() {
        val streamsConfig = StreamsSinkConfiguration.from(config)
        if (!streamsConfig.enabled) {
            return
        }
        log.info("Starting the Kafka Sink")
        this.eventConsumer = getEventConsumerFactory()
                .createStreamsEventConsumer(config.raw, log)
                .withTopics(streamsTopicService.getTopics())
        this.eventConsumer.start()
        this.job = createJob()
        log.info("Kafka Sink started")
    }

    override fun stop() = runBlocking {
        log.info("Stopping Sink daemon Job")
        try {
            job.cancelAndJoin()
        } catch (e : UninitializedPropertyAccessException) { /* ignoring this one only */ }
    }

    override fun getEventSinkConfigMapper(): StreamsEventSinkConfigMapper {
        return object: StreamsEventSinkConfigMapper(streamsConfigMap, mappingKeys) {
            override fun convert(config: Map<String, String>): Map<String, String> {
                val props = streamsConfigMap
                        .toMutableMap()
                props += config.mapKeys { mappingKeys.getOrDefault(it.key, it.key) }
                return props
            }

        }
    }

    private fun createJob(): Job {
        log.info("Creating Sink daemon Job")
        return GlobalScope.launch(Dispatchers.IO) { // TODO improve exception management
            try {
                while (isActive) {
                    eventConsumer.read { topic, data ->
                        if (log.isDebugEnabled) {
                            log.debug("Reading data from topic $topic")
                        }
                        queryExecution.writeForTopic(topic, data)
                    }
                }
                eventConsumer.stop()
            } catch (e: Throwable) {
                val message = e.message ?: "Generic error, please check the stack trace: "
                log.error(message, e)
                eventConsumer.stop()
            }
        }
    }

}

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
                .associateBy({ it.first }, { it.second })

        fun fromMap(map: Map<String, Any>): KafkaTopicConfig {
            val commit = map.getOrDefault("commit", true).toString().toBoolean()
            val topicPartitionsMap = toTopicPartitionMap(map
                    .getOrDefault("partitions", emptyMap<String, List<Map<String, Any>>>()) as Map<String, List<Map<String, Any>>>)
            return KafkaTopicConfig(commit = commit, topicPartitionsMap = topicPartitionsMap)
        }
    }
}

open class KafkaAutoCommitEventConsumer(private val config: KafkaSinkConfiguration,
                                        private val log: Log): StreamsEventConsumer(log) {

    private var isSeekSet = false

    val consumer = KafkaConsumer<String, ByteArray>(config.asProperties())

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
        StreamsUtils.ignoreExceptions({ consumer.close() }, UninitializedPropertyAccessException::class.java)
    }

    private fun readSimple(action: (String, List<Any>) -> Unit) {
        val records = consumer.poll(0)
        if (!records.isEmpty) {
            try {
                this.topics.forEach { topic ->
                    val topicRecords = records.records(topic)
                    if (!topicRecords.iterator().hasNext()) {
                        return@forEach
                    }
                    action(topic, topicRecords.map { JSONUtils.readValue<Any>(it.value()) })
                }
            } catch (e: Exception) {
                // TODO add dead letter queue
            }
        }
    }

    private fun readFromPartition(config: KafkaTopicConfig, action: (String, List<Any>) -> Unit) {
        setSeek(config.topicPartitionsMap)
        val records = consumer.poll(0)
        val consumerRecordsMap = toConsumerRecordsMap(config.topicPartitionsMap, records)
        if (consumerRecordsMap.isNotEmpty()) {
            try {
                consumerRecordsMap.forEach { action(it.key.topic(), it.value.map { JSONUtils.readValue<Any>(it.value()) }) }
            } catch (e: Exception) {
                // TODO add dead letter queue
            }
        }
    }

    override fun read(topicConfig: Map<String, Any>, action: (String, List<Any>) -> Unit) {
        val kafkaTopicConfig = KafkaTopicConfig.fromMap(topicConfig)
        if (kafkaTopicConfig.topicPartitionsMap.isEmpty()) {
            readSimple(action)
        } else {
            readFromPartition(kafkaTopicConfig, action)
        }
    }

    fun toConsumerRecordsMap(topicPartitionsMap: Map<TopicPartition, Long>,
                             records: ConsumerRecords<String, ByteArray>)
            : Map<TopicPartition, List<ConsumerRecord<String, ByteArray>>> = topicPartitionsMap
            .mapValues {
                records.records(it.key)
            }
            .filterValues { it.isNotEmpty() }

    fun setSeek(topicPartitionsMap: Map<TopicPartition, Long>) {
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

class KafkaManualCommitEventConsumer(private val config: KafkaSinkConfiguration,
                                     private val log: Log): KafkaAutoCommitEventConsumer(config, log) {

    private val topicPartitionOffsetMap = ConcurrentHashMap<TopicPartition, OffsetAndMetadata>()

    override fun start() {
        if (topics.isEmpty()) {
            log.info("No topics specified Kafka Consumer will not started")
            return
        }
        this.consumer.subscribe(topics, StreamsConsumerRebalanceListener(topicPartitionOffsetMap, consumer, config.autoOffsetReset, log))
    }

    private fun readSimple(action: (String, List<Any>) -> Unit): Map<TopicPartition, OffsetAndMetadata> {
        val topicMap = mutableMapOf<TopicPartition, OffsetAndMetadata>()
        val records = consumer.poll(0)
        if (!records.isEmpty) {
            this.topics.forEach { topic ->
                val topicRecords = records.records(topic)
                if (!topicRecords.iterator().hasNext()) {
                    return@forEach
                }
                val lastRecord = topicRecords.last()
                val offsetAndMetadata = OffsetAndMetadata(lastRecord.offset(), "")
                val topicPartition = TopicPartition(lastRecord.topic(), lastRecord.partition())
                topicMap[topicPartition] = offsetAndMetadata
                topicPartitionOffsetMap[topicPartition] = offsetAndMetadata
                try {
                    action(topic, topicRecords.map { JSONUtils.readValue<Any>(it.value()) })
                } catch (e: Exception) {
                    // TODO add dead letter queue
                }
            }
        }
        return topicMap
    }

    private fun readFromPartition(kafkaTopicConfig: KafkaTopicConfig,
                                  action: (String, List<Any>) -> Unit): Map<TopicPartition, OffsetAndMetadata> {
        val topicMap = mutableMapOf<TopicPartition, OffsetAndMetadata>()
        setSeek(kafkaTopicConfig.topicPartitionsMap)
        val records = consumer.poll(0)
        val consumerRecordsMap = toConsumerRecordsMap(kafkaTopicConfig.topicPartitionsMap, records)
        if (consumerRecordsMap.isNotEmpty()) {
            try {
                consumerRecordsMap.forEach {
                    val lastRecord = it.value.last()
                    val topicPartition = TopicPartition(lastRecord.topic(), lastRecord.partition())
                    val offsetAndMetadata = OffsetAndMetadata(lastRecord.offset(), "")
                    topicMap[topicPartition] = offsetAndMetadata
                    topicPartitionOffsetMap[topicPartition] = offsetAndMetadata
                    action(it.key.topic(), it.value.map { JSONUtils.readValue<Any>(it.value()) })
                }
            } catch (e: Exception) {
                // TODO add dead letter queue
            }
        }
        return topicMap
    }

    private fun commitData(commit: Boolean, topicMap: Map<TopicPartition, OffsetAndMetadata>) {
        if (commit && topicMap.isNotEmpty()) {
            consumer.commitSync(topicMap)
        }
    }

    override fun read(topicConfig: Map<String, Any>, action: (String, List<Any>) -> Unit) {
        val kafkaTopicConfig = KafkaTopicConfig.fromMap(topicConfig)
        val topicMap = if (kafkaTopicConfig.topicPartitionsMap.isEmpty()) {
            readSimple(action)
        } else {
            readFromPartition(kafkaTopicConfig, action)
        }
        commitData(kafkaTopicConfig.commit, topicMap)
    }
}

class StreamsConsumerRebalanceListener(private val topicPartitionOffsetMap: Map<TopicPartition, OffsetAndMetadata>,
                                       private val consumer: KafkaConsumer<String, ByteArray>,
                                       private val autoOffsetReset: String,
                                       private val log: Log): ConsumerRebalanceListener {

    override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
        val offsets = partitions
                .map {
                    val offset = consumer.position(it)
                    if (log.isDebugEnabled) {
                        log.debug("onPartitionsRevoked: for topic ${it.topic()} partition ${it.partition()}, the last saved offset is: $offset")
                    }
                    it to OffsetAndMetadata(offset, "")
                }
                .toMap()
        consumer.commitSync(offsets)
    }

    override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
        for (partition in partitions) {
            val offset = (topicPartitionOffsetMap[partition] ?: consumer.committed(partition))?.offset()
            if (log.isDebugEnabled) {
                log.debug("onPartitionsAssigned: for ${partition.topic()} partition ${partition.partition()}, the retrieved offset is: $offset")
            }
            if (offset == null) {
                when (autoOffsetReset) {
                    "latest" -> consumer.seekToEnd(listOf(partition))
                    "earliest" -> consumer.seekToBeginning(listOf(partition))
                    else -> throw RuntimeException("No kafka.auto.offset.reset property specified")
                }
            } else {
                consumer.seek(partition, offset + 1)
            }
        }
    }
}