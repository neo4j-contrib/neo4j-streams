package streams.kafka

import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import streams.*
import streams.service.errors.ErrorService
import streams.service.errors.KafkaDLQService
import streams.utils.Neo4jUtils
import java.util.concurrent.TimeUnit


class KafkaEventSink(private val config: Config,
                     private val queryExecution: StreamsEventSinkQueryExecution,
                     private val streamsTopicService: StreamsTopicService,
                     private val log: Log,
                     private val db: GraphDatabaseAPI): StreamsEventSink(config, queryExecution, streamsTopicService, log, db) {

    private lateinit var eventConsumer: StreamsEventConsumer
    private lateinit var job: Job

    override val streamsConfigMap = config.raw.filterKeys {
        it.startsWith("kafka.") || (it.startsWith("streams.") && !it.startsWith("streams.sink.topic.cypher."))
    }.toMap()

    override val mappingKeys = mapOf(
            "zookeeper" to "kafka.zookeeper.connect",
            "broker" to "kafka.${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}",
            "from" to "kafka.${ConsumerConfig.AUTO_OFFSET_RESET_CONFIG}",
            "autoCommit" to "kafka.${ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG}",
            "groupId" to "kafka.${ConsumerConfig.GROUP_ID_CONFIG}")

    override fun getEventConsumerFactory(): StreamsEventConsumerFactory {
        return object: StreamsEventConsumerFactory() {
            override fun createStreamsEventConsumer(config: Map<String, String>, log: Log): StreamsEventConsumer {
                val kafkaConfig = KafkaSinkConfiguration.from(config)

                val errorService = KafkaDLQService(kafkaConfig.asProperties(), ErrorService.ErrorConfig.from(kafkaConfig.streamsSinkConfiguration.errorConfig),{ s, e -> log.error(s,e as Throwable)})
                return if (kafkaConfig.enableAutoCommit) {
                    KafkaAutoCommitEventConsumer(kafkaConfig, log, errorService)
                } else {
                    KafkaManualCommitEventConsumer(kafkaConfig, log, errorService)
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

