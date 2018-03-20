package kafka

import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.codehaus.jackson.map.ObjectMapper
import org.neo4j.graphdb.event.TransactionData
import org.neo4j.graphdb.event.TransactionEventHandler
import org.neo4j.logging.Log
import java.io.IOException
import java.lang.System.currentTimeMillis
import java.util.concurrent.ThreadLocalRandom

class KafkaModule(private val log: Log, private val producer: Producer<Long, ByteArray>, private val config: KafkaConfiguration) : TransactionEventHandler<Any> {
    override fun afterCommit(data: TransactionData?, state: Any?) {}

    override fun afterRollback(data: TransactionData?, state: Any?) {}

    // todo batch send, see: https://kafka.apache.org/10/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html
    internal fun sendRecordToKafka(partition: Int, node: NodeRecord) {
        try {
            val producerRecord = ProducerRecord(config.topic, partition, currentTimeMillis(), node.id, objectMapper.writeValueAsBytes(node))

            producer.send(producerRecord,
//                    { meta, error -> error?.let { log.warn("an exception has occurred while sending record in partition ${meta.partition()} offset ${meta.offset()}") } })
                    { meta, error -> log.warn("sending record in partition ${meta.partition()} offset ${meta.offset()} data ${meta.topic()} key size ${meta.serializedKeySize()}", error) })

        } catch (ioe: IOException) {
            log.error("error sending record to partition: $partition, with document id: ${node.id}", ioe)
        }
    }

    override fun beforeCommit(transactionData: TransactionData): Any? {
        val data = mutableMapOf<Long, NodeRecord>()

        transactionData.deletedNodes().forEach { node ->
            data.put(node.id, NodeRecord(node.id, state = UpdateState.deleted))
        }

        transactionData.createdNodes().forEach { node ->
            data.computeIfAbsent(node.id, { NodeRecord(node, UpdateState.created) })
        }

        transactionData.assignedNodeProperties().forEach { p ->
        data.computeIfAbsent(p.entity().id, { NodeRecord(p.entity(), UpdateState.updated) })
        }

        val partition = ThreadLocalRandom.current().nextInt(config.partitionSize)
        data.values.forEach { sendRecordToKafka(partition, it) }
        return null
    }

    companion object {
        private val objectMapper = ObjectMapper()
    }
}
