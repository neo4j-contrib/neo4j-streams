package integrations.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.hamcrest.Matchers
import org.junit.Test
import org.neo4j.function.ThrowingSupplier
import streams.Assert
import streams.KafkaTestUtils
import streams.extensions.execute
import streams.serialization.JSONUtils
import streams.setConfig
import streams.start
import java.util.UUID
import java.util.concurrent.TimeUnit

class KafkaEventSinkCommitTSE : KafkaEventSinkBaseTSE() {
    @Test
    fun `should write last offset with auto commit false`() {
        val topic = UUID.randomUUID().toString()
        db.setConfig("streams.sink.topic.cypher.$topic", cypherQueryTemplate)
        db.setConfig("kafka.${ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG}", "false")
        db.start()
        val partition = 0
        var producerRecord = ProducerRecord(topic, partition, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(data))
        kafkaProducer.send(producerRecord).get()
        val newData = data.toMutableMap()
        newData["id"] = 2
        producerRecord = ProducerRecord(topic, partition, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(newData))
        val resp = kafkaProducer.send(producerRecord).get()

        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val kafkaConsumer = KafkaTestUtils.createConsumer<String, ByteArray>(
                    bootstrapServers = KafkaEventSinkSuiteIT.kafka.bootstrapServers,
                    schemaRegistryUrl = KafkaEventSinkSuiteIT.schemaRegistry.getSchemaRegistryUrl())
            val offsetAndMetadata = kafkaConsumer.committed(TopicPartition(topic, partition))
            kafkaConsumer.close()
            if (offsetAndMetadata == null) {
                false
            } else {
                val query = "MATCH (n:Label) RETURN count(*) AS count"
                db.execute(query) {
                    val result = it.columnAs<Long>("count")
                    result.hasNext() && result.next() == 2L && !result.hasNext() && resp.offset() + 1 == offsetAndMetadata.offset()
                }
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)

    }

    @Test
    fun `should fix issue 186 with auto commit false`() {
        val product = "product" to "MERGE (p:Product {id: event.id}) ON CREATE SET p.name = event.name"
        val customer = "customer" to "MERGE (c:Customer {id: event.id}) ON CREATE SET c.name = event.name"
        val bought = "bought" to """
            MERGE (c:Customer {id: event.id})
            MERGE (p:Product {id: event.id})
            MERGE (c)-[:BOUGHT]->(p)
        """.trimIndent()
        db.setConfig("streams.sink.topic.cypher.${product.first}", product.second)
        db.setConfig("streams.sink.topic.cypher.${customer.first}", customer.second)
        db.setConfig("streams.sink.topic.cypher.${bought.first}", bought.second)
        db.setConfig("kafka.${ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG}", "false")
        db.start()

        val props = mapOf("id" to 1, "name" to "My Awesome Product")
        var producerRecord = ProducerRecord(product.first, UUID.randomUUID().toString(),
                JSONUtils.writeValueAsBytes(props))
        kafkaProducer.send(producerRecord).get()
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = """
                MATCH (p:Product)
                WHERE properties(p) = ${'$'}props
                RETURN count(p) AS count
            """.trimIndent()
            db.execute(query, mapOf("props" to props)) {
                val result = it.columnAs<Long>("count")
                result.hasNext() && result.next() == 1L && !result.hasNext()
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }
}