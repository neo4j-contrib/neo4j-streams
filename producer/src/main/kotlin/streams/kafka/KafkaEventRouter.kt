package streams.kafka

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.AuthorizationException
import org.apache.kafka.common.errors.OutOfOrderSequenceException
import org.apache.kafka.common.errors.ProducerFencedException
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.logging.LogService
import org.neo4j.logging.Log
import streams.NodeRoutingConfiguration
import streams.RelationshipRoutingConfiguration
import streams.StreamsEventRouter
import streams.StreamsEventRouterConfiguration
import streams.events.EntityType
import streams.events.StreamsEvent
import streams.events.StreamsTransactionEvent
import streams.serialization.JacksonUtil
import java.lang.Exception
import java.util.*
import java.util.concurrent.ThreadLocalRandom



class KafkaEventRouter: StreamsEventRouter {
    private val log: Log
    private lateinit var producer: Producer<Long, ByteArray>
    private val kafkaConfig: KafkaConfiguration


    constructor(logService: LogService, config: Config): super(logService, config) {
        log = logService.getUserLog(KafkaEventRouter::class.java)
        log.info("Initializing Kafka Connector")
        kafkaConfig = KafkaConfiguration.from(config.raw)
    }

    override fun start() {
        val props = kafkaConfig.asProperties()
        producer = Neo4jKafkaProducer<Long, ByteArray>(props)
        producer.initTransactions()
        log.info("Kafka Connector started")
    }

    override fun stop() {
        producer.close()
    }

    private fun send(producerRecord: ProducerRecord<Long, ByteArray>) {
        producer.send(producerRecord,
                { meta: RecordMetadata?, error: Exception? ->
                    if (log.isDebugEnabled) {
                        log.debug("Sent record in partition ${meta?.partition()} offset ${meta?.offset()} data ${meta?.topic()} key size ${meta?.serializedKeySize()}", error)
                    }
                })
    }

    private fun sendEvent(partition: Int, topic: String, event: StreamsEvent) {
        if (log.isDebugEnabled) {
            log.debug("Trying to send a simple event with payload ${event.payload} to kafka")
        }
        val producerRecord = ProducerRecord(topic, partition, System.currentTimeMillis(), event.hashCode().toLong(),
                JacksonUtil.getMapper().writeValueAsBytes(event))
        send(producerRecord)
    }

    private fun sendEvent(partition: Int, topic: String, event: StreamsTransactionEvent) {
        if (log.isDebugEnabled) {
            log.debug("Trying to send a transaction event with txId ${event.meta.txId} and txEventId ${event.meta.txEventId} to kafka")
        }
        val producerRecord = ProducerRecord(topic, partition, System.currentTimeMillis(), event.meta.txId + event.meta.txEventId,
                JacksonUtil.getMapper().writeValueAsBytes(event))
        send(producerRecord)
    }

    override fun sendEvents(topic: String, transactionEvents: List<out StreamsEvent>) {
        try {
            producer.beginTransaction()
            transactionEvents.forEach {
                val partition = ThreadLocalRandom.current().nextInt(kafkaConfig.numPartitions)
                if (it is StreamsTransactionEvent) {
                    sendEvent(partition, topic, it)
                } else {
                    sendEvent(partition, topic, it)
                }
            }
            producer.commitTransaction()
        } catch (e: ProducerFencedException) {
            log.error("Another producer with the same transactional.id has been started. Stack trace is:", e)
            producer.close()
        } catch (e: OutOfOrderSequenceException) {
            log.error("The broker received an unexpected sequence number from the producer. Stack trace is:", e)
            producer.close()
        } catch (e: AuthorizationException) {
            log.error("Error in authorization. Stack trace is:", e)
            producer.close()
        } catch (e: KafkaException) {
            log.error("Generic kafka error. Stack trace is:", e)
            producer.abortTransaction()
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