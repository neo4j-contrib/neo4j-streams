package streams.kafka

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.codehaus.jackson.map.ObjectMapper
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import streams.StreamsEventSink
import streams.StreamsEventSinkQueryExecution
import streams.StreamsTopicService
import streams.utils.Neo4jUtils
import streams.utils.StreamsUtils

class KafkaEventSink: StreamsEventSink {

    private val log: Log
    private val kafkaConfig: KafkaSinkConfiguration
    private val db: GraphDatabaseAPI

    private val objectMapper: ObjectMapper = ObjectMapper()

    private lateinit var job: Job
    private lateinit var queryExecution: StreamsEventSinkQueryExecution
    private lateinit var kafkaConsumer: KafkaConsumer<String, ByteArray>

    override var streamsTopicService: StreamsTopicService? = null

    constructor(config: Config,
                db: GraphDatabaseAPI): super(config, db) {
        this.log = Neo4jUtils.getLogService(db)
                .getUserLog(KafkaEventSink::class.java)
        log.info("Initializing Kafka Sink Connector")
        this.kafkaConfig = KafkaSinkConfiguration.from(config)
        this.db = db
    }

    override fun available() {
        this.streamsTopicService = StreamsTopicService(this.db, kafkaConfig.streamsSinkConfiguration)
        if (streamsTopicService!!.getTopics().isEmpty()) {
            log.info("No topic configuration found under streams.sink.topic.*, Kafka Sink will not stared")
            return
        }

        this.queryExecution = StreamsEventSinkQueryExecution(this.streamsTopicService!!, db, log)
        createConsumer();

        job = createJob()
        log.info("Kafka Sink Connector started.")
    }

    private fun createConsumer() {
        this.kafkaConsumer = KafkaConsumer(kafkaConfig.asProperties())
        kafkaConsumer.subscribe(streamsTopicService!!.getTopics())
        if (log.isDebugEnabled) {
            log.debug("Subscribing topics: ${streamsTopicService!!.getAll()}")
        } else {
            log.info("Subscribing topics: ${streamsTopicService!!.getTopics()}")
        }
    }

    private fun createJob(): Job {
        return GlobalScope.launch(Dispatchers.IO) {
            try {
                while (true) {
                    val records = StreamsUtils.ignoreExceptions({
                        kafkaConsumer.poll(kafkaConfig.streamsSinkConfiguration.sinkPollingInterval)
                    })
                    if (records != null) {
                        consume(records)
                    }
                }
            } catch (e: Throwable) {
                val message = e.message ?: "Generic error, please check the stack trace: "
                log.error(message, e)
            } finally {
                kafkaConsumer.close()
            }
        }
    }

    private fun consume(records: ConsumerRecords<String, ByteArray>) {
        streamsTopicService!!.getTopics().forEach {
            if (log.isDebugEnabled) {
                log.debug("Reading data from topic $it")
            }
            val list = records.records(it)
                    .map {
                        objectMapper.readValue(it.value(), Map::class.java)
                                .mapKeys { it.key.toString() }
                    }
            if (list.isNotEmpty()) {
                if (log.isDebugEnabled) {
                    log.debug("Reading data from topic $it, with data $list")
                }
                queryExecution.execute(it, list)
            }
        }
    }

    override fun stop() {
        StreamsUtils.ignoreExceptions({ job.cancel() }, UninitializedPropertyAccessException::class.java)
        StreamsUtils.ignoreExceptions({ kafkaConsumer.wakeup() }, UninitializedPropertyAccessException::class.java,
                WakeupException::class.java)
    }

}