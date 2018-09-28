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

class RecordPublisher(private val log: Log, private val producer: Producer<Long, ByteArray>, val config: KafkaConfiguration) {
    // todo batch send, see: https://kafka.apache.org/10/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html
    fun sendRecordsToKafka(partition: Int, topic: String, node: NodeRecord) {
        try {
            val producerRecord = ProducerRecord(topic, partition, currentTimeMillis(), node.id, objectMapper.writeValueAsBytes(node))

            producer.send(producerRecord,
                    { meta, error -> log.warn("sending record in partition ${meta.partition()} offset ${meta.offset()} data ${meta.topic()} key size ${meta.serializedKeySize()}", error) })

        } catch (ioe: IOException) {
            log.error("error sending record to partition: $partition, with document id: ${node.id}", ioe)
        }
    }

    fun publish(data: Map<Long, Map<String, NodeRecord>>) {
        val partition = ThreadLocalRandom.current().nextInt(config.partitionSize)
        data.values.forEach { records -> records.forEach { (topic, record) -> sendRecordsToKafka(partition, topic, record) } }
    }

    companion object {
        private val objectMapper = ObjectMapper()
    }

}
class KafkaModule(val publisher: RecordPublisher, val config: KafkaConfiguration = publisher.config) : TransactionEventHandler<Any> {
    override fun afterCommit(data: TransactionData?, state: Any?) {}

    override fun afterRollback(data: TransactionData?, state: Any?) {}

    override fun beforeCommit(transactionData: TransactionData): Any? {
        // todo special case for default pattern (single topic, all labels, all props)

        val data = mutableMapOf<Long, Map<String,NodeRecord>>()

        transactionData.deletedNodes().forEach { node ->
            data.put(node.id, NodePattern.forNode(config.patterns, node).associate { it.topic to NodeRecord(node.id, state = UpdateState.deleted)})
        }

        transactionData.createdNodes().forEach { node ->
            data.computeIfAbsent(node.id, { NodePattern.toNodeRecords(node, UpdateState.created, config.patterns) })
        }

        transactionData.assignedNodeProperties().forEach { p ->
        data.computeIfAbsent(p.entity().id, { NodePattern.toNodeRecords(p.entity(), UpdateState.updated, config.patterns) })
        }

        publisher.publish(data)
        return null
    }
}
