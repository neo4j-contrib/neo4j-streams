package streams.kafka

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import streams.StreamsEventConsumer
import streams.StreamsEventConsumerFactory
import streams.StreamsEventSink
import streams.StreamsEventSinkAvailabilityListener
import streams.StreamsEventSinkQueryExecution
import streams.StreamsSinkConfiguration
import streams.StreamsSinkConfigurationConstants
import streams.StreamsTopicService
import streams.events.StreamsPluginStatus
import streams.serialization.JSONUtils
import streams.utils.KafkaValidationUtils.getInvalidTopicsError
import streams.utils.Neo4jUtils
import streams.utils.StreamsUtils
import java.util.concurrent.TimeUnit


class KafkaEventSink(private val config: Config,
                     private val queryExecution: StreamsEventSinkQueryExecution,
                     private val streamsTopicService: StreamsTopicService,
                     private val log: Log,
                     private val db: GraphDatabaseAPI): StreamsEventSink(config, queryExecution, streamsTopicService, log, db) {

    private val mutex = Mutex()

    private lateinit var eventConsumer: StreamsEventConsumer

    private lateinit var job: Job

    private val isNeo4jCluster = Neo4jUtils.isCluster(db, log)

    private val streamsConfig = StreamsSinkConfiguration.from(config)

    override val streamsConfigMap = config.raw.filterKeys {
        it.startsWith("kafka.") || (it.startsWith("streams.") && !it.startsWith("streams.sink.topic.cypher."))
    }.toMap()

    override val mappingKeys = mapOf(
            "zookeeper" to "kafka.zookeeper.connect",
            "broker" to "kafka.${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}",
            "from" to "kafka.${ConsumerConfig.AUTO_OFFSET_RESET_CONFIG}",
            "autoCommit" to "kafka.${ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG}",
            "keyDeserializer" to "kafka.${ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG}",
            "valueDeserializer" to "kafka.${ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG}",
            "schemaRegistryUrl" to "kafka.schema.registry.url",
            "groupId" to "kafka.${ConsumerConfig.GROUP_ID_CONFIG}")

    override fun getEventConsumerFactory(): StreamsEventConsumerFactory {
        return object: StreamsEventConsumerFactory() {
            override fun createStreamsEventConsumer(config: Map<String, String>, log: Log, topics: Set<Any>): StreamsEventConsumer {
                val kafkaConfig = KafkaSinkConfiguration.from(config)
                return if (kafkaConfig.enableAutoCommit) {
                    KafkaAutoCommitEventConsumer(kafkaConfig, log, topics as Set<String>)
                } else {
                    KafkaManualCommitEventConsumer(kafkaConfig, log, topics as Set<String>)
                }
            }
        }
    }

    override fun start() = runBlocking { // TODO move to the abstract class
        val isWriteableInstance = Neo4jUtils.isWriteableInstance(db, isNeo4jCluster)
        val topics = streamsTopicService.getTopics()
        if (streamsConfig.clusterOnly && !isNeo4jCluster) {
            if (log.isDebugEnabled) {
                log.info("""
                        |Cannot init the Kafka Sink module as is forced to work only in a cluster env, 
                        |please check the value of `streams.${StreamsSinkConfigurationConstants.CLUSTER_ONLY}`
                    """.trimMargin())
            }
            return@runBlocking
        }
        if (!streamsConfig.enabled) {
            if (topics.isNotEmpty() && isWriteableInstance) {
                log.warn("You configured the following topics: $topics, in order to make the Sink work please set the `${StreamsSinkConfigurationConstants.STREAMS_CONFIG_PREFIX}${StreamsSinkConfigurationConstants.ENABLED}` to `true`")
            }
            return@runBlocking
        }
        mutex.withLock {
            if (StreamsPluginStatus.RUNNING == status()) {
                if (log.isDebugEnabled) {
                    log.debug("Kafka Sink is already started.")
                }
                return@runBlocking
            }
            log.info("Starting the Kafka Sink")
            eventConsumer = getEventConsumerFactory()
                    .createStreamsEventConsumer(config.raw, log, streamsTopicService.getTopics())
            job = createJob()
        }
        if (isWriteableInstance) {
            if (log.isDebugEnabled) {
                streamsTopicService.getAll().forEach {
                    log.debug("Subscribed topics for type ${it.key} are: ${it.value}")
                }
            } else {
                log.info("Subscribed topics: $topics")
            }
        }
        log.info("Kafka Sink started")
    }

    override fun stop() = runBlocking { // TODO move to the abstract class
        log.info("Stopping Kafka Sink daemon Job")
        mutex.withLock {
            if (status() == StreamsPluginStatus.STOPPED) {
                return@runBlocking
            }
            try {
                job.cancelAndJoin()
                log.info("Kafka Sink daemon Job stopped")
            } catch (e: Exception) {
                when (e) {
                    is UninitializedPropertyAccessException, is CancellationException -> Unit
                    else -> throw e
                }
            }
        }
    }

    private fun createJob(): Job {
        log.info("Creating Sink daemon Job")
        return GlobalScope.launch(Dispatchers.IO) { // TODO improve exception management
            try {
                eventConsumer.start()
                while (isActive) {
                    val timeMillis = if (Neo4jUtils.isWriteableInstance(db, isNeo4jCluster)) {
                        eventConsumer.read { topic, data ->
                            if (log.isDebugEnabled) {
                                log.debug("Reading data from topic $topic")
                            }
                            queryExecution.writeForTopic(topic, data)
                        }
                        streamsConfig.pollInterval
                    } else {
                        val timeMillis = streamsConfig.checkWriteableInstanceInterval
                        if (log.isDebugEnabled) {
                            log.debug("Not in a writeable instance, new check in $timeMillis millis")
                        }
                        timeMillis
                    }
                    delay(timeMillis)
                }
            } catch (e: Throwable) {
                val message = e.message ?: "Generic error, please check the stack trace: "
                log.error(message, e)
            } finally {
                eventConsumer.stop()
            }
        }
    }

    override fun printInvalidTopics() {
        StreamsUtils.ignoreExceptions({
            if (eventConsumer.invalidTopics().isNotEmpty()) {
                log.warn(getInvalidTopicsError(eventConsumer.invalidTopics()))
            }
        }, UninitializedPropertyAccessException::class.java)
    }

    override fun status(): StreamsPluginStatus = when (this::job.isInitialized && this.job.isActive) {
        true -> StreamsPluginStatus.RUNNING
        else -> StreamsPluginStatus.STOPPED
    }

}
