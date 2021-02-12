package integrations.kafka

import integrations.kafka.KafkaTestUtils.createConsumer
import kotlinx.coroutines.runBlocking
import extension.newDatabase
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.hamcrest.Matchers
import org.junit.Test
import org.neo4j.function.ThrowingSupplier
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.test.assertion.Assert
import streams.serialization.JSONUtils
import java.util.*
import java.util.concurrent.TimeUnit

class KafkaEventSinkCommit : KafkaEventSinkBase() {
    @Test
    fun shouldWriteLastOffsetWithNoAutoCommit() = runBlocking {
        val topic = UUID.randomUUID().toString()
        graphDatabaseBuilder.setConfig("streams.sink.topic.cypher.$topic", cypherQueryTemplate)
        graphDatabaseBuilder.setConfig("kafka.${ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG}", "false")
        db = graphDatabaseBuilder.newDatabase() as GraphDatabaseAPI
        val partition = 0
        var producerRecord = ProducerRecord(topic, partition, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(data))
        kafkaProducer.send(producerRecord).get()
        val newData = data.toMutableMap()
        newData["id"] = 2
        producerRecord = ProducerRecord(topic, partition, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(newData))
        val resp = kafkaProducer.send(producerRecord).get()

        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = "MATCH (n:Label) RETURN count(*) AS count"
            val result = db.execute(query).columnAs<Long>("count")

            val kafkaConsumer = createConsumer<String, ByteArray>(
                    kafka = KafkaEventSinkSuiteIT.kafka,
                    schemaRegistry = KafkaEventSinkSuiteIT.schemaRegistry)
            val offsetAndMetadata = kafkaConsumer.committed(TopicPartition(topic, partition))
            kafkaConsumer.close()

            result.hasNext() && result.next() == 2L && !result.hasNext() && resp.offset() + 1 == offsetAndMetadata.offset()
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun shouldWriteLastOffsetWithAsyncCommit() = runBlocking {
        val topic = UUID.randomUUID().toString()
        graphDatabaseBuilder.setConfig("streams.sink.topic.cypher.$topic", cypherQueryTemplate)
        graphDatabaseBuilder.setConfig("kafka.${ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG}", "false")
        graphDatabaseBuilder.setConfig("kafka.streams.commit.async", "true")
        db = graphDatabaseBuilder.newDatabase() as GraphDatabaseAPI
        val partition = 0
        var producerRecord = ProducerRecord(topic, partition, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(data))
        kafkaProducer.send(producerRecord).get()
        val newData = data.toMutableMap()
        newData["id"] = 2
        producerRecord = ProducerRecord(topic, partition, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(newData))
        val resp = kafkaProducer.send(producerRecord).get()

        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = "MATCH (n:Label) RETURN count(*) AS count"
            val result = db.execute(query).columnAs<Long>("count")

            val kafkaConsumer = createConsumer<String, ByteArray>(
                    kafka = KafkaEventSinkSuiteIT.kafka,
                    schemaRegistry = KafkaEventSinkSuiteIT.schemaRegistry)
            val offsetAndMetadata = kafkaConsumer.committed(TopicPartition(topic, partition))
            kafkaConsumer.close()

            result.hasNext() && result.next() == 2L && !result.hasNext() && resp.offset() + 1 == offsetAndMetadata?.offset()
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)

    }

    @Test
    fun `should fix issue 186 with auto commit false`() = runBlocking {
        val product = "product" to "MERGE (p:Product {id: event.id}) ON CREATE SET p.name = event.name"
        val customer = "customer" to "MERGE (c:Customer {id: event.id}) ON CREATE SET c.name = event.name"
        val bought = "bought" to """
            MERGE (c:Customer {id: event.id})
            MERGE (p:Product {id: event.id})
            MERGE (c)-[:BOUGHT]->(p)
        """.trimIndent()
        graphDatabaseBuilder.setConfig("streams.sink.topic.cypher.${product.first}", product.second)
        graphDatabaseBuilder.setConfig("streams.sink.topic.cypher.${customer.first}", customer.second)
        graphDatabaseBuilder.setConfig("streams.sink.topic.cypher.${bought.first}", bought.second)
        graphDatabaseBuilder.setConfig("kafka.${ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG}", "false")
        db = graphDatabaseBuilder.newDatabase() as GraphDatabaseAPI

        val props = mapOf("id" to 1, "name" to "My Awesome Product")
        var producerRecord = ProducerRecord(product.first, UUID.randomUUID().toString(),
                JSONUtils.writeValueAsBytes(props))
        kafkaProducer.send(producerRecord).get()
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = """
                MATCH (p:Product)
                WHERE properties(p) = {props}
                RETURN count(p) AS count
            """.trimIndent()
            val result = db.execute(query, mapOf("props" to props)).columnAs<Long>("count")
            result.hasNext() && result.next() == 1L && !result.hasNext()
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }
}