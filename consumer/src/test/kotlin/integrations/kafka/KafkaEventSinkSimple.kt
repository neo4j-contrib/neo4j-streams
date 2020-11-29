package integrations.kafka

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import extension.newDatabase
import org.apache.kafka.clients.producer.ProducerRecord
import org.hamcrest.Matchers
import org.junit.Test
import org.neo4j.function.ThrowingSupplier
import org.neo4j.graphdb.Node
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.test.assertion.Assert
import streams.events.StreamsPluginStatus
import streams.procedures.StreamsSinkProcedures
import streams.serialization.JSONUtils
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.stream.Collectors
import kotlin.test.assertEquals

class KafkaEventSinkSimple: KafkaEventSinkBase() {

    @Test
    fun shouldWriteDataFromSink() {
        val topic = UUID.randomUUID().toString()
        graphDatabaseBuilder.setConfig("streams.sink.topic.cypher.$topic", cypherQueryTemplate)
        db = graphDatabaseBuilder.newDatabase() as GraphDatabaseAPI

        val producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(data))
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
                |MATCH (n:Label) WHERE properties(n) = {props}
                |RETURN count(*) AS count""".trimMargin()
            val result = db.execute(query, mapOf("props" to props)).columnAs<Long>("count")
            result.hasNext() && result.next() == 1L && !result.hasNext()
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun shouldNotWriteDataFromSinkWithNoTopicLoaded() = runBlocking {
        db = graphDatabaseBuilder.newDatabase(StreamsPluginStatus.STOPPED) as GraphDatabaseAPI

        val producerRecord = ProducerRecord(UUID.randomUUID().toString(), UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(data))
        kafkaProducer.send(producerRecord).get()

        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = """
                |MATCH (n:Label)
                |RETURN n""".trimMargin()
            val result = db.execute(query).columnAs<Node>("n")
            result.hasNext()
        }, Matchers.equalTo(false), 30, TimeUnit.SECONDS)
    }

    @Test
    fun shouldNotStartInASingleInstance() {
        val topic = UUID.randomUUID().toString()
        db = graphDatabaseBuilder
                .setConfig("streams.sink.topic.cypher.$topic", cypherQueryTemplate)
                .setConfig("streams.cluster.only", "true")
                .newDatabase(StreamsPluginStatus.STOPPED) as GraphDatabaseAPI
        db.dependencyResolver.resolveDependency(Procedures::class.java)
                .registerProcedure(StreamsSinkProcedures::class.java, true)

        val expectedRunning = listOf(mapOf("name" to "status", "value" to StreamsPluginStatus.STOPPED.toString()))

        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            try {
                // when
                val result = db.execute("CALL streams.sink.status()")

                // then
                val actual = result.stream()
                        .collect(Collectors.toList())
                expectedRunning == actual
            } catch (e: Exception) {
                false
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
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

    @Test
    fun `should stop and start the sink via procedures`() = runBlocking {
        // given
        val topic = UUID.randomUUID().toString()
        graphDatabaseBuilder.setConfig("streams.sink.topic.cypher.$topic", cypherQueryTemplate)
        db = graphDatabaseBuilder.newDatabase() as GraphDatabaseAPI
        db.dependencyResolver.resolveDependency(Procedures::class.java)
                .registerProcedure(StreamsSinkProcedures::class.java, true)

        val stopped = db.execute("CALL streams.sink.stop()")
        org.junit.Assert.assertEquals(mapOf("name" to "status", "value" to StreamsPluginStatus.STOPPED.toString()), stopped.next())
        org.junit.Assert.assertFalse(stopped.hasNext())

        val producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(data))
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

        delay(30000)

        val query = """MATCH (n:Label) WHERE properties(n) = {props}
                |RETURN count(*) AS count""".trimMargin()
        val result = db.execute(query, mapOf("props" to props)).columnAs<Long>("count")
        org.junit.Assert.assertEquals(0L, result.next())

        // when
        val started = db.execute("CALL streams.sink.start()")
        org.junit.Assert.assertEquals(mapOf("name" to "status", "value" to StreamsPluginStatus.RUNNING.toString()), started.next())
        org.junit.Assert.assertFalse(started.hasNext())

        // then
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val result = db.execute(query, mapOf("props" to props)).columnAs<Long>("count")
            result.hasNext() && result.next() == 1L && !result.hasNext()
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun `neo4j should start normally in case kafka is not reachable`() {
        // given
        val topic = UUID.randomUUID().toString()
        db = graphDatabaseBuilder.setConfig("streams.sink.topic.cypher.$topic", cypherQueryTemplate)
                .setConfig("kafka.bootstrap.servers", "foo")
                .setConfig("kafka.default.api.timeout.ms", "5000")
                .newDatabase(StreamsPluginStatus.STOPPED) as GraphDatabaseAPI
        db.dependencyResolver.resolveDependency(Procedures::class.java)
                .registerProcedure(StreamsSinkProcedures::class.java, true)

        val expectedRunning = listOf(mapOf("name" to "status", "value" to StreamsPluginStatus.STOPPED.toString()))

        // when
        val result = db.execute("CALL streams.sink.status()")

        // then
        val actual = result.stream()
                .collect(Collectors.toList())
        assertEquals(expectedRunning, actual)
    }
}