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
import org.apache.kafka.common.errors.WakeupException
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import streams.StreamsEventConsumer
import streams.StreamsEventConsumerFactory
import streams.StreamsEventSink
import streams.StreamsEventSinkQueryExecution
import streams.StreamsSinkConfiguration
import streams.config.StreamsConfig
import streams.events.StreamsPluginStatus
import streams.StreamsTopicService
import streams.utils.KafkaValidationUtils.getInvalidTopicsError
import streams.utils.Neo4jUtils
import streams.utils.StreamsUtils
import java.util.concurrent.TimeUnit


class KafkaEventSink(private val config: StreamsConfig,
                     private val queryExecution: StreamsEventSinkQueryExecution,
                     private val streamsTopicService: StreamsTopicService,
                     private val log: Log,
                     private val db: GraphDatabaseAPI): StreamsEventSink(config, queryExecution, streamsTopicService, log, db) {

    private val mutex = Mutex()
    private val streamsConfig = StreamsSinkConfiguration.from(config, db.databaseName())

    private lateinit var eventConsumer: KafkaEventConsumer
    private lateinit var job: Job

    override val streamsConfigMap = config.config.filterKeys {
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
            override fun createStreamsEventConsumer(config: StreamsConfig, log: Log): StreamsEventConsumer {
                val kafkaConfig = KafkaSinkConfiguration.from(config, db.databaseName())
                return if (kafkaConfig.enableAutoCommit) {
                    KafkaAutoCommitEventConsumer(kafkaConfig, log)
                } else {
                    KafkaManualCommitEventConsumer(kafkaConfig, log)
                }
            }
        }
    }

    override fun start() = runBlocking { // TODO move to the abstract class
        val topics = streamsTopicService.getTopics()
        val isWriteableInstance = Neo4jUtils.isWriteableInstance(db)
        if (streamsConfig.clusterOnly && !Neo4jUtils.isCluster(db)) {
            if (log.isDebugEnabled) {
                log.info("""
                        |Cannot init the Kafka Sink module as is forced to work only in a cluster env, 
                        |please check the value of `${StreamsConfig.CLUSTER_ONLY}`
                    """.trimMargin())
            }
            return@runBlocking
        }
        if (!streamsConfig.enabled) {
            if (topics.isNotEmpty() && isWriteableInstance) {
                log.warn("You configured the following topics: $topics, in order to make the Sink work please set ${StreamsConfig.SINK_ENABLED}=true")
            }
            log.info("The Kafka Sink is disabled")
            return@runBlocking
        }
        log.info("Starting the Kafka Sink")
        mutex.withLock {
            if (StreamsPluginStatus.RUNNING == status()) {
                if (log.isDebugEnabled) {
                    log.debug("Kafka Sink is already started.")
                }
                return@runBlocking
            }
            try {
                eventConsumer = getEventConsumerFactory()
                        .createStreamsEventConsumer(config, log)
                        .withTopics(topics) as KafkaEventConsumer
                job = createJob()
            } catch (e: Exception) {
                log.error("Cannot create job because of the following exception:", e)
                return@runBlocking
            }
        }
        if (isWriteableInstance) {
            if (log.isDebugEnabled) {
                streamsTopicService.getAll().forEach {
                    log.debug("Subscribed topics for type ${it.key} are: ${it.value}")
                }
            } else {
                log.info("Subscribed topics: $topics")
            }
        } else {
            if (log.isDebugEnabled) {
                log.info("Not a writeable instance")
            }
        }
        log.info("Kafka Sink started")
    }

    override fun stop() = runBlocking { // TODO move to the abstract class
        log.info("Stopping Kafka Sink daemon Job")
        mutex.withLock {
            StreamsUtils.ignoreExceptions({
                runBlocking {
                    if (job.isActive) {
                        eventConsumer.wakeup()
                        job.cancelAndJoin()
                    }
                    log.info("Kafka Sink daemon Job stopped")
                }
            }, UninitializedPropertyAccessException::class.java)
        }
        Unit
    }

    private fun createJob(): Job {
        log.info("Creating Sink daemon Job")
        return GlobalScope.launch(Dispatchers.IO) { // TODO improve exception management
            try {
                eventConsumer.start()
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
                        val timeMillis = streamsConfig.checkWriteableInstanceInterval
                        if (log.isDebugEnabled) {
                            log.debug("Not in a writeable instance, new check in $timeMillis millis")
                        }
                        timeMillis
                    }
                    delay(timeMillis)
                }
            } catch (e: Exception) {
                when (e) {
                    is CancellationException, is WakeupException -> null
                    else -> {
                        val message = e.message ?: "Generic error, please check the stack trace: "
                        log.error(message, e)
                    }
                }
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

