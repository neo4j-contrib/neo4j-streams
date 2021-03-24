package streams.kafka

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.AuthorizationException
import org.apache.kafka.common.errors.OutOfOrderSequenceException
import org.apache.kafka.common.errors.ProducerFencedException
import org.neo4j.logging.Log
import streams.StreamsEventRouter
import streams.StreamsEventRouterConfiguration
import streams.asSourceRecordKey
import streams.asSourceRecordValue
import streams.events.StreamsEvent
import streams.events.StreamsPluginStatus
import streams.events.StreamsTransactionEvent
import streams.serialization.JSONUtils
import streams.toMap
import streams.utils.KafkaValidationUtils.getInvalidTopicsError
import streams.utils.StreamsUtils
import java.util.Properties
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom


class KafkaEventRouter(private val config: Map<String, String>, private val log: Log): StreamsEventRouter(config, log) {
    override val eventRouterConfiguration: StreamsEventRouterConfiguration = StreamsEventRouterConfiguration.from(config, log)

    private val mutex = Mutex()

    private var producer: Neo4jKafkaProducer<ByteArray, ByteArray>? = null
    private val kafkaConfig by lazy { KafkaConfiguration.from(config) }
    private val kafkaAdminService by lazy { KafkaAdminService(kafkaConfig, eventRouterConfiguration.allTopics(), log) }

    override fun printInvalidTopics() {
        val invalidTopics = kafkaAdminService.getInvalidTopics()
        if (invalidTopics.isNotEmpty()) {
            log.warn(getInvalidTopicsError(invalidTopics))
        }
    }

    private fun status(producer: Neo4jKafkaProducer<*, *>?): StreamsPluginStatus = when (producer != null) {
        true -> StreamsPluginStatus.RUNNING
        else -> StreamsPluginStatus.STOPPED
    }

    override fun start() = runBlocking {
        mutex.withLock(producer) {
            if (status(producer) == StreamsPluginStatus.RUNNING) {
                return@runBlocking
            }
            log.info("Initialising Kafka Connector")
            kafkaAdminService.start()
            val props = kafkaConfig.asProperties()
            producer = Neo4jKafkaProducer(props)
            producer!!.initTransactions()
            log.info("Kafka Connector started")
        }
    }

    override fun stop() = runBlocking {
        mutex.withLock(producer) {
            if (status(producer) == StreamsPluginStatus.STOPPED) {
                return@runBlocking
            }
            StreamsUtils.ignoreExceptions({ producer?.flush() }, UninitializedPropertyAccessException::class.java)
            StreamsUtils.ignoreExceptions({ producer?.close() }, UninitializedPropertyAccessException::class.java)
            StreamsUtils.ignoreExceptions({ kafkaAdminService.stop() }, UninitializedPropertyAccessException::class.java)
            producer = null
        }
    }

    private fun send(producerRecord: ProducerRecord<ByteArray?, ByteArray?>, sync: Boolean = false): Map<String, Any>? {
        if (!kafkaAdminService.isValidTopic(producerRecord.topic())) {
            // TODO add logging system here
            return null
        }
        return if (sync) {
            producer?.send(producerRecord)?.get()?.toMap()
        } else {
            producer?.send(producerRecord) { meta, error ->
                if (meta != null && log.isDebugEnabled) {
                    log.debug("Successfully sent record in partition ${meta?.partition()} offset ${meta?.offset()} data ${meta?.topic()} key size ${meta?.serializedKeySize()}")
                }
                if (error != null) {
                    if (log.isDebugEnabled) {
                        log.debug("Error while sending record to ${producerRecord.topic()}, because of the following exception:", error)
                    }
                    // TODO add logging system here
                }
            }
            null
        }
    }

    // this method is used by the procedures
    private fun sendEvent(topic: String, event: StreamsEvent, config: Map<String, Any?>, sync: Boolean = false): Map<String, Any>? {
        if (log.isDebugEnabled) {
            log.debug("Trying to send a simple event with payload ${event.payload} to kafka")
        }
        // in the procedures we allow to define a custom message key via the configuration property key
        // in order to have the backwards compatibility we define as default value the old key
        val key = config.getOrDefault("key", UUID.randomUUID().toString())
        val partition = (config["partition"])?.toString()?.toInt()
        val producerRecord = ProducerRecord(topic, partition, System.currentTimeMillis(), key?.let { JSONUtils.writeValueAsBytes(it) },
                JSONUtils.writeValueAsBytes(event))
        return send(producerRecord, sync)
    }

    // this method is used by the transaction event handler
    private fun sendEvent(topic: String, event: StreamsTransactionEvent, config: Map<String, Any?>) {
        if (log.isDebugEnabled) {
            log.debug("Trying to send a transaction event with txId ${event.meta.txId} and txEventId ${event.meta.txEventId} to kafka")
        }
        val key = JSONUtils.writeValueAsBytes(event.asSourceRecordKey(kafkaConfig.streamsLogCompactionStrategy))
        val value = event.asSourceRecordValue(kafkaConfig.streamsLogCompactionStrategy)?.let { JSONUtils.writeValueAsBytes(it) }
        val producerRecord = ProducerRecord(topic, null, System.currentTimeMillis(), key, value)
        send(producerRecord)
    }

    override fun sendEventsSync(topic: String, transactionEvents: List<out StreamsEvent>, config: Map<String, Any?>): List<Map<String, Any>> {
        producer?.beginTransaction()

        val results = transactionEvents.mapNotNull {
            sendEvent(topic, it, config, true)
        }
        producer?.commitTransaction()

        return results
    }

    override fun sendEvents(topic: String, transactionEvents: List<out StreamsEvent>, config: Map<String, Any?>) {
        try {
            producer?.beginTransaction()
            transactionEvents.forEach {
                if (it is StreamsTransactionEvent) {
                    sendEvent(topic, it, config)
                } else {
                    sendEvent(topic, it, config)
                }
            }
            producer?.commitTransaction()
        } catch (e: ProducerFencedException) {
            log.error("Another producer with the same transactional.id has been started. Stack trace is:", e)
            producer?.close()
        } catch (e: OutOfOrderSequenceException) {
            log.error("The broker received an unexpected sequence number from the producer?. Stack trace is:", e)
            producer?.close()
        } catch (e: AuthorizationException) {
            log.error("Error in authorization. Stack trace is:", e)
            producer?.close()
        } catch (e: KafkaException) {
            log.error("Generic kafka error. Stack trace is:", e)
            producer?.abortTransaction()
        }
    }

}

class Neo4jKafkaProducer<K, V>: KafkaProducer<K, V> {
    private val isTransactionEnabled: Boolean
    constructor(props: Properties): super(props) {
        isTransactionEnabled = props.containsKey("transactional.id")
    }

    override fun initTransactions() {
        if (isTransactionEnabled) {
            super.initTransactions()
        }
    }

    override fun beginTransaction() {
        if (isTransactionEnabled) {
            super.beginTransaction()
        }
    }

    override fun commitTransaction() {
        if (isTransactionEnabled) {
            super.commitTransaction()
        }
    }

    override fun abortTransaction() {
        if (isTransactionEnabled) {
            super.abortTransaction()
        }
    }

}