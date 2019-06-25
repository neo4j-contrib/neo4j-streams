package streams.kafka

import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import streams.*
import streams.extensions.offsetAndMetadata
import streams.extensions.topicPartition
import streams.serialization.JSONUtils
import streams.utils.Neo4jUtils
import java.util.concurrent.TimeUnit


class KafkaEventSink(private val config: Config,
                     private val queryExecution: StreamsEventSinkQueryExecution,
                     private val streamsTopicService: StreamsTopicService,
                     private val log: Log,
                     private val db: GraphDatabaseAPI): StreamsEventSink(config, queryExecution, streamsTopicService, log, db) {

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

    override fun start() { // TODO move to the abstract class
        val streamsConfig = StreamsSinkConfiguration.from(config)
        val topics = streamsTopicService.getTopics()
        val isWriteableInstance = Neo4jUtils.isWriteableInstance(db)
        if (!streamsConfig.enabled) {
            if (topics.isNotEmpty() && isWriteableInstance) {
                log.warn("You configured the following topics: $topics, in order to make the Sink work please set the `${StreamsSinkConfigurationConstants.STREAMS_CONFIG_PREFIX}${StreamsSinkConfigurationConstants.ENABLED}` to `true`")
            }
            return
        }
        log.info("Starting the Kafka Sink")
        this.eventConsumer = getEventConsumerFactory()
                .createStreamsEventConsumer(config.raw, log)
                .withTopics(topics)
        this.eventConsumer.start()
        this.job = createJob()
        if (isWriteableInstance) {
            if (log.isDebugEnabled) {
                log.debug("Subscribed topics with Cypher queries: ${streamsTopicService.getAllCypherTemplates()}")
                log.debug("Subscribed topics with CDC configuration: ${streamsTopicService.getAllCDCTopics()}")
            } else {
                log.info("Subscribed topics: $topics")
            }
        }
        log.info("Kafka Sink started")
    }

    override fun stop() = runBlocking { // TODO move to the abstract class
        log.info("Stopping Kafka Sink daemon Job")
        try {
            job.cancelAndJoin()
            log.info("Kafka Sink daemon Job stopped")
        } catch (e : UninitializedPropertyAccessException) { /* ignoring this one only */ }
    }

    override fun getEventSinkConfigMapper(): StreamsEventSinkConfigMapper { // TODO move to the abstract class
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
                    val timeMillis = if (Neo4jUtils.isWriteableInstance(db)) {
                        eventConsumer.read { topic, data ->
                            if (log.isDebugEnabled) {
                                log.debug("Reading data from topic $topic")
                            }
                            queryExecution.writeForTopic(topic, data)
                        }
                        TimeUnit.SECONDS.toMillis(1)
                    } else {
                        val timeMillis = TimeUnit.MINUTES.toMillis(5)
                        if (log.isDebugEnabled) {
                            log.debug("Not in a writeable instance, new check in $timeMillis millis")
                        }
                        timeMillis
                    }
                    delay(timeMillis)
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
        consumer.close()
    }

    private fun readSimple(action: (String, List<Any>) -> Unit) {
        val records = consumer.poll(0)
        this.topics
                .filter { topic -> records.records(topic).iterator().hasNext() }
                .map { topic -> topic to records.records(topic) }
                .forEach { (topic, topicRecords) -> executeAction(action, topic, topicRecords) }
    }

    fun executeAction(action: (String, List<Any>) -> Unit, topic: String, topicRecords: MutableIterable<ConsumerRecord<String, ByteArray>>) {
        try {
            action(topic, convert(topicRecords))
        } catch (e: Exception) {
            // TODO send to the DLQ
        }
    }

    private fun convert(topicRecords: MutableIterable<ConsumerRecord<String, ByteArray>>) = topicRecords
            .map {
                try {
                    "ok" to JSONUtils.readValue<Any>(it.value())
                } catch (e: Exception) {
                    "error" to it
                }
            }
            .groupBy({ it.first }, { it.second })
            .let {
                // TODO send content of the "error" key to the DLQ
                it.getOrDefault("ok", emptyList())
            }

    private fun readFromPartition(config: KafkaTopicConfig, action: (String, List<Any>) -> Unit) {
        setSeek(config.topicPartitionsMap)
        val records = consumer.poll(0)
        config.topicPartitionsMap
                .mapValues { records.records(it.key) }
                .filterValues { it.isNotEmpty() }
                .mapKeys { it.key.topic() }
                .forEach { (topic, topicRecords) -> executeAction(action, topic, topicRecords) }
    }

    override fun read(action: (String, List<Any>) -> Unit) {
        readSimple(action)
    }

    override fun read(topicConfig: Map<String, Any>, action: (String, List<Any>) -> Unit) {
        val kafkaTopicConfig = KafkaTopicConfig.fromMap(topicConfig)
        if (kafkaTopicConfig.topicPartitionsMap.isEmpty()) {
            readSimple(action)
        } else {
            readFromPartition(kafkaTopicConfig, action)
        }
    }

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

    override fun start() {
        if (topics.isEmpty()) {
            log.info("No topics specified Kafka Consumer will not started")
            return
        }
        this.consumer.subscribe(topics)
    }

    private fun readSimple(action: (String, List<Any>) -> Unit): Map<TopicPartition, OffsetAndMetadata> {
        val records = consumer.poll(0)
        return this.topics
                .filter { topic -> records.records(topic).iterator().hasNext() }
                .map { topic -> topic to records.records(topic) }
                .map { (topic, topicRecords) ->
                    executeAction(action, topic, topicRecords)
                    topicRecords.last()
                }
                .map { it.topicPartition() to it.offsetAndMetadata() }
                .toMap()
    }

    private fun readFromPartition(kafkaTopicConfig: KafkaTopicConfig,
                                  action: (String, List<Any>) -> Unit): Map<TopicPartition, OffsetAndMetadata> {
        setSeek(kafkaTopicConfig.topicPartitionsMap)
        val records = consumer.poll(0)
        return kafkaTopicConfig.topicPartitionsMap
                .mapValues { records.records(it.key) }
                .filterValues { it.isNotEmpty() }
                .mapKeys { it.key.topic() }
                .map { (topic, topicRecords) ->
                    executeAction(action, topic, topicRecords)
                    topicRecords.last()
                }
                .map { it.topicPartition() to it.offsetAndMetadata() }
                .toMap()
    }

    private fun commitData(commit: Boolean, topicMap: Map<TopicPartition, OffsetAndMetadata>) {
        if (commit) {
            consumer.commitSync(topicMap)
        }
    }

    override fun read(action: (String, List<Any>) -> Unit) {
        val topicMap = readSimple(action)
        commitData(true, topicMap)
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