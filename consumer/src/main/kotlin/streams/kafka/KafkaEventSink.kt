package streams.kafka

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.selects.whileSelect
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.neo4j.kernel.configuration.Config
import org.neo4j.logging.Log
import streams.*
import streams.serialization.JSONUtils
import streams.utils.Neo4jUtils
import streams.utils.StreamsUtils
import streams.utils.TopicRecordQueue
import java.util.*

class KafkaEventSink(private val config: Config,
                     private val queryExecution: StreamsEventSinkQueryExecution,
                     private val streamsTopicService: StreamsTopicService,
                     private val log: Log): StreamsEventSink(config, queryExecution, streamsTopicService, log) {

    private lateinit var eventConsumer: StreamsEventConsumer<*>
    private lateinit var readJob: Job
    private lateinit var writeJob: Job
    private lateinit var streamsSinkConfiguration: StreamsSinkConfiguration

    private val topicRecordQueue = TopicRecordQueue<Any>()

    private val streamsConfigMap = config.raw.filterKeys {
        it.startsWith("kafka.") || (it.startsWith("streams.") && !it.startsWith("streams.sink.topic.cypher."))
    }.toMap()

    override fun getEventConsumerFactory(): StreamsEventConsumerFactory {
        return object: StreamsEventConsumerFactory() {
            override fun createStreamsEventConsumer(config: Map<String, String>, log: Log): StreamsEventConsumer<*> {
                val kafkaConfig = KafkaSinkConfiguration.from(config)
                val kafkaConsumer = KafkaConsumer<String, ByteArray>(kafkaConfig.asProperties())
                return KafkaEventConsumer(kafkaConsumer, kafkaConfig.streamsSinkConfiguration, log)
            }
        }
    }

    private val mappingKeys = mapOf("from" to "kafka.auto.offset.reset",
            "timeout" to "streams.sink.batch.timeout",
            "batchSize" to "streams.sink.batch.size")

    override fun start() {
        streamsSinkConfiguration = StreamsSinkConfiguration.from(config)
        if (!streamsSinkConfiguration.enabled) {
            log.info("The sink will not started, please set the property streams.sink.enabled=true")
            return
        }
        log.info("Starting the Kafka Sink")
        val topics = streamsTopicService.getAll()
        this.eventConsumer = getEventConsumerFactory()
                .createStreamsEventConsumer(config.raw, log)
                .withTopics(topics.keys)
        this.eventConsumer.start()
        this.readJob = createReadJob()
        this.writeJob = createWriteJob()
        log.info("Kafka Sink started")
    }

    override fun stop() = runBlocking {
        log.info("Stopping Sink daemon Job")
        try {
            readJob.cancelAndJoin()
        } catch (e: UninitializedPropertyAccessException) { /* ignoring this one only */ }
        try {
            writeJob.cancelAndJoin()
        } catch (e: UninitializedPropertyAccessException) { /* ignoring this one only */ }
        eventConsumer.stop()
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

    private fun createReadJob(): Job {
        log.info("Creating Sink read daemon Job")
        return GlobalScope.launch {
            while (isActive) {
                val data = eventConsumer.read()
                if (data.isNullOrEmpty()) {
                    continue
                }
                topicRecordQueue.addMap(data)
            }
        }
    }

    private fun createWriteJob(): Job {
        log.info("Creating Sink write daemon Job")
        return GlobalScope.launch {
            while (isActive) {
                val deferredList = topicRecordQueue
                        .map { (topic, queue) ->
                            async {
                                val data = queue.getBatchOrTimeout(streamsSinkConfiguration.batchSize, streamsSinkConfiguration.batchTimeout)
                                if (data.isNotEmpty()) {
                                    queryExecution.execute(topic, data)
                                }
                            }
                        }
                if (deferredList.isEmpty()) {
                    continue
                }
                val timeout = streamsSinkConfiguration.queryTimeout
                val ticker = ticker(timeout)
                whileSelect {
                    ticker.onReceive {
                        if (log.isDebugEnabled) {
                            log.debug("Timeout $timeout occurred while executing queries")
                        }
                        deferredList.forEach { deferred -> deferred.cancel() }
                        false // Stops the whileSelect
                    }
                    val isAllCompleted = deferredList.all { it.isCompleted } // when all are completed
                    deferredList.forEach {
                        it.onAwait { !isAllCompleted } // Stops the whileSelect
                    }
                }
            }
        }
    }
}

class KafkaEventConsumer(private val consumer: KafkaConsumer<String, ByteArray>,
                         private val config: StreamsSinkConfiguration,
                         private val log: Log): StreamsEventConsumer<KafkaConsumer<String, ByteArray>>(consumer, config, log) {

    private lateinit var topics: Set<String>

    private var isRunning = false

    override fun withTopics(topics: Set<String>): StreamsEventConsumer<KafkaConsumer<String, ByteArray>> {
        this.topics = topics
        return this
    }

    override fun start() {
        if (topics.isEmpty()) {
            log.info("No topics specified Kafka Consumer will not started")
            return
        }
        this.consumer.subscribe(topics)
        isRunning = true
    }

    override fun stop() {
        isRunning = false
        StreamsUtils.ignoreExceptions({ consumer.close() }, UninitializedPropertyAccessException::class.java)
    }

    override fun read(timeout: Long): Map<String, List<Any>>? {
        if (!isRunning) {
            return null
        }
        val records = consumer.poll(timeout)
        if (records != null && !records.isEmpty) {
            return records
                    .map {
                        it.topic()!! to JSONUtils.readValue(it.value(), Any::class.java)
                    }
                    .groupBy({ it.first }, { it.second })
        }
        return null
    }

    override fun read(): Map<String, List<Any>>? {
        return read(0L)
    }
}