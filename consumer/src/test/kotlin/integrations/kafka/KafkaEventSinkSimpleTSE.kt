package integrations.kafka

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.producer.ProducerRecord
import org.hamcrest.Matchers
import org.junit.Test
import org.neo4j.function.ThrowingSupplier
import org.neo4j.graphdb.Node
import org.neo4j.kernel.api.procedure.GlobalProcedures
import streams.Assert
import streams.events.StreamsPluginStatus
import streams.extensions.execute
import streams.procedures.StreamsSinkProcedures
import streams.utils.JSONUtils
import streams.setConfig
import streams.start
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.streams.toList
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class KafkaEventSinkSimpleTSE: KafkaEventSinkBaseTSE() {

    private val topics = listOf("shouldWriteCypherQuery")

    @Test
    fun shouldWriteDataFromSink() = runBlocking {
        db.setConfig("streams.sink.topic.cypher.shouldWriteCypherQuery", cypherQueryTemplate)
        db.start()

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
        db.start()

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
        db.setConfig("streams.sink.topic.cypher.${product.first}", product.second)
        db.setConfig("streams.sink.topic.cypher.${customer.first}", customer.second)
        db.setConfig("streams.sink.topic.cypher.${bought.first}", bought.second)
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

    @Test
    fun `should stop and start the sink via procedures`() = runBlocking {
        // given
        db.setConfig("streams.sink.topic.cypher.shouldWriteCypherQuery", cypherQueryTemplate)
        db.start()
        db.dependencyResolver.resolveDependency(GlobalProcedures::class.java)
                .registerProcedure(StreamsSinkProcedures::class.java, true)

        db.execute("CALL streams.sink.stop()") { stopped ->
            assertEquals(mapOf("name" to "status", "value" to StreamsPluginStatus.STOPPED.toString()), stopped.next())
            assertFalse { stopped.hasNext() }
        }

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

        delay(30000)

        val query = """MATCH (n:Label) WHERE properties(n) = ${'$'}props
                |RETURN count(*) AS count""".trimMargin()
        db.execute(query, mapOf("props" to props)) {
            val result = it.columnAs<Long>("count")
            assertEquals(0L, result.next())
        }


        // when
        db.execute("CALL streams.sink.start()") { started ->
            assertEquals(mapOf("name" to "status", "value" to StreamsPluginStatus.RUNNING.toString()), started.next())
            assertFalse(started.hasNext())
        }

        // then
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            db.execute(query, mapOf("props" to props)) {
                val result = it.columnAs<Long>("count")
                result.hasNext() && result.next() == 1L && !result.hasNext()
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun shouldNotStartInASingleInstance() {
        db.setConfig("streams.sink.topic.cypher.shouldWriteCypherQuery", cypherQueryTemplate)
                .setConfig("streams.cluster.only", "true")
                .start()
        db.dependencyResolver.resolveDependency(GlobalProcedures::class.java)
                .registerProcedure(StreamsSinkProcedures::class.java, true)

        val expectedRunning = listOf(mapOf("name" to "status", "value" to StreamsPluginStatus.STOPPED.toString()))

        // when
        val actual = db.execute("CALL streams.sink.status()") {
            it.stream().toList()
        }

        // then
        assertEquals(expectedRunning, actual)
    }

    @Test
    fun `neo4j should start normally in case kafka is not reachable`() {
        db.setConfig("streams.sink.topic.cypher.shouldWriteCypherQuery", cypherQueryTemplate)
                .setConfig("kafka.bootstrap.servers", "foo")
                .setConfig("kafka.default.api.timeout.ms", "5000")
                .start()
        db.dependencyResolver.resolveDependency(GlobalProcedures::class.java)
                .registerProcedure(StreamsSinkProcedures::class.java, true)

        val expectedRunning = listOf(mapOf("name" to "status", "value" to StreamsPluginStatus.STOPPED.toString()))

        // when
        val actual = db.execute("CALL streams.sink.status()") {
            it.stream().toList()
        }

        // then
        assertEquals(expectedRunning, actual)
    }
}