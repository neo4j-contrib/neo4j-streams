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
import streams.events.EntityType
import streams.events.StreamsEvent
import streams.serialization.JacksonUtil
import java.lang.Exception
import java.util.*
import java.util.concurrent.ThreadLocalRandom



class KafkaEventRouter: StreamsEventRouter {
    private val log: Log
    private val producer: Producer<Long, ByteArray>
    private val kafkaConfig: KafkaConfiguration

    private val configPrefix = "kafka."

    constructor(logService: LogService, config: Config): super(logService, config) {
        log = logService.getUserLog(KafkaEventRouter::class.java)
        log.info("Initializing Kafka Connector")
        kafkaConfig = KafkaConfiguration.from(config.raw.filterKeys { it.startsWith(configPrefix) }.mapKeys { it.key.substring(configPrefix.length) })
        val props = kafkaConfig.asProperties()

        producer = Neo4jKafkaProducer<Long, ByteArray>(props)
        producer.initTransactions()
        log.info("Kafka initialization successful")
        val topics = kafkaConfig.nodeRouting.map { it.topic }.toMutableSet() + kafkaConfig.relRouting.map { it.topic }.toMutableSet()
        AdminClient.create(props).use { client -> client.createTopics(
                topics.map { topic -> NewTopic(topic, kafkaConfig.numPartitions, kafkaConfig.replication.toShort()) }) }
        log.info("Kafka Connector started.")
    }

    override fun sendEvent(event: StreamsEvent) {
        val events = when (event.payload.type) {
            EntityType.node -> NodeRoutingConfiguration.prepareEvent(event, kafkaConfig.nodeRouting)
            EntityType.relationship -> RelationshipRoutingConfiguration.prepareEvent(event, kafkaConfig.relRouting)
        }
        try {
            if (log.isDebugEnabled) {
                log.debug("Trying to send the event with txId ${event.meta.txId} to kafka")
            }
            producer.beginTransaction()
            events.forEach {
                val partition = ThreadLocalRandom.current().nextInt(kafkaConfig.numPartitions)
                val producerRecord = ProducerRecord(it.key, partition, System.currentTimeMillis(), it.value.meta.txId + it.value.meta.txEventId,
                        JacksonUtil.getMapper().writeValueAsBytes(it))
                producer.send(producerRecord,
                        { meta: RecordMetadata?, error: Exception? ->
                            if (log.isDebugEnabled) {
                                log.debug("sending record in partition ${meta?.partition()} offset ${meta?.offset()} data ${meta?.topic()} key size ${meta?.serializedKeySize()}", error)
                            }
                        })
            }
            producer.commitTransaction()
            if (log.isDebugEnabled) {
                log.debug("Event with txId ${event.meta.txId} sent successfully")
            }
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