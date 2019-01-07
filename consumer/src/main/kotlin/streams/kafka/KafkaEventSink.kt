package streams.kafka

import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.neo4j.kernel.configuration.Config
import org.neo4j.logging.Log
import streams.*
import streams.serialization.JSONUtils
import streams.utils.StreamsUtils

class KafkaEventSink(private val config: Config,
                     private val queryExecution: StreamsEventSinkQueryExecution,
                     private val streamsTopicService: StreamsTopicService,
                     private val log: Log): StreamsEventSink(config, queryExecution, streamsTopicService, log) {

    private lateinit var eventConsumer: StreamsEventConsumer<*>
    private lateinit var job: Job

    private val streamsConfigMap = config.raw.filterKeys {
        it.startsWith("kafka.") || (it.startsWith("streams.") && !it.startsWith("streams.sink.topic.cypher."))
    }.toMap()

    private val mappingKeys = mapOf("timeout" to "streams.sink.polling.interval",
            "from" to "kafka.auto.offset.reset")

    override fun getEventConsumerFactory(): StreamsEventConsumerFactory {
        return object: StreamsEventConsumerFactory() {
            override fun createStreamsEventConsumer(config: Map<String, String>, log: Log): StreamsEventConsumer<*> {
                val kafkaConfig = KafkaSinkConfiguration.from(config)
                val kafkaConsumer = KafkaConsumer<String, ByteArray>(kafkaConfig.asProperties())
                return KafkaEventConsumer(kafkaConsumer, kafkaConfig.streamsSinkConfiguration, log)
            }
        }
    }

    override fun start() {
        val streamsConfig = StreamsSinkConfiguration.from(config)
        if (!streamsConfig.enabled) {
            log.info("The sink will not started, please set the property streams.sink.enabled=true")
            return
        }
        log.info("Starting the Kafka Sink")
        val topics = streamsTopicService.getAll()
        this.eventConsumer = getEventConsumerFactory()
                .createStreamsEventConsumer(config.raw, log)
                .withTopics(topics.keys)
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
        return GlobalScope.launch(Dispatchers.IO) {
            try {
                while (isActive) {
                    val data= eventConsumer.read()
                    data?.forEach {
                        if (log.isDebugEnabled) {
                            log.debug("Reading data from topic ${it.key}, with data ${it.value}")
                        }
                        queryExecution.execute(it.key, it.value)
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

class KafkaEventConsumer(private val consumer: KafkaConsumer<String, ByteArray>,
                         private val config: StreamsSinkConfiguration,
                         private val log: Log): StreamsEventConsumer<KafkaConsumer<String, ByteArray>>(consumer, config, log) {

    private lateinit var topics: Set<String>

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
    }

    override fun stop() {
        StreamsUtils.ignoreExceptions({ consumer.close() }, UninitializedPropertyAccessException::class.java)
    }

    override fun read(): Map<String, List<Any>>? {
        val records = consumer.poll(config.sinkPollingInterval)
        if (records != null && !records.isEmpty) {
            return records
                    .map {
                        it.topic()!! to JSONUtils.readValue(it.value(), Any::class.java)
                    }
                    .groupBy({ it.first }, { it.second })
        }
        return null
    }
}