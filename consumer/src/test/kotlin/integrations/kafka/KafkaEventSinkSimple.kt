package integrations.kafka

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.producer.ProducerRecord
import org.hamcrest.Matchers
import org.junit.Test
import org.neo4j.function.ThrowingSupplier
import org.neo4j.graphdb.Node
import org.neo4j.test.assertion.Assert
import streams.extensions.execute
import streams.serialization.JSONUtils
import streams.setConfig
import streams.start
import java.util.UUID
import java.util.concurrent.TimeUnit

class KafkaEventSinkSimple: KafkaEventSinkBase() {

    private val topics = listOf("shouldWriteCypherQuery")

    @Test
    fun shouldWriteDataFromSink() = runBlocking {
        graphDatabaseBuilder.setConfig("streams.sink.topic.cypher.shouldWriteCypherQuery", cypherQueryTemplate)
        db = graphDatabaseBuilder.start()

        val producerRecord = ProducerRecord(topics[0], UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(data))
        kafkaProducer.send(producerRecord).get()
        val props = data
                .flatMap {
                    if (it.key == "properties") {
                        val map = it.value as Map<String, Any>
                        map.entries.map { it.key to it.value }
                    } else {
                        listOf(it.key to it.value)
                    }
                }
                .toMap()

        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = """
                |MATCH (n:Label) WHERE properties(n) = ${'$'}props
                |RETURN count(*) AS count""".trimMargin()
            db.execute(query, mapOf("props" to props)) {
                val result = it.columnAs<Long>("count")
                result.hasNext() && result.next() == 1L && !result.hasNext()
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)

    }

    @Test
    fun shouldNotWriteDataFromSinkWithNoTopicLoaded() = runBlocking {
        db = graphDatabaseBuilder.start()

        val producerRecord = ProducerRecord(topics[0], UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(data))
        kafkaProducer.send(producerRecord).get()
        delay(5000)

        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = """
                |MATCH (n:Label)
                |RETURN n""".trimMargin()
            db.execute(query) {
                val result = it.columnAs<Node>("n")
                result.hasNext()
            }
        }, Matchers.equalTo(false), 30, TimeUnit.SECONDS)
    }

    @Test
    fun `should fix issue 186 with auto commit true`() {
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
        db = graphDatabaseBuilder.start()

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