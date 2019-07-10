package integrations.kafka

import kotlinx.coroutines.*
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.junit.Test
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.kernel.internal.GraphDatabaseAPI
import streams.procedures.StreamsSinkProcedures
import streams.serialization.JSONUtils
import java.util.*
import kotlin.test.*

@Suppress("UNCHECKED_CAST", "DEPRECATION")
class KafkaStreamsSinkProcedures : KafkaEventSinkBase() {

    private fun testProcedure(topic: String) {
        val producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(data))
        kafkaProducer.send(producerRecord).get()
        val result = db.execute("CALL streams.consume('$topic', {timeout: 5000}) YIELD event RETURN event")
        assertTrue { result.hasNext() }
        val resultMap = result.next()
        assertTrue { resultMap.containsKey("event") }
        assertNotNull(resultMap["event"], "should contain event")
        val event = resultMap["event"] as Map<String, Any?>
        val resultData = event["data"] as Map<String, Any?>
        assertEquals(data, resultData)
    }

    @Test
    fun shouldConsumeDataFromProcedureWithSinkDisabled() {
        graphDatabaseBuilder.setConfig("streams.sink.enabled", "false")
        db = graphDatabaseBuilder.newGraphDatabase() as GraphDatabaseAPI
        db.dependencyResolver.resolveDependency(Procedures::class.java)
                .registerProcedure(StreamsSinkProcedures::class.java, true)
        val topic = "bar"
        testProcedure(topic)
    }

    @Test
    fun shouldConsumeDataFromProcedure() {
        db = graphDatabaseBuilder.newGraphDatabase() as GraphDatabaseAPI
        db.dependencyResolver.resolveDependency(Procedures::class.java)
                .registerProcedure(StreamsSinkProcedures::class.java, true)
        val topic = "foo"
        testProcedure(topic)
    }

    @Test
    fun shouldTimeout() {
        db = graphDatabaseBuilder.newGraphDatabase() as GraphDatabaseAPI
        db.dependencyResolver.resolveDependency(Procedures::class.java)
                .registerProcedure(StreamsSinkProcedures::class.java, true)
        val result = db.execute("CALL streams.consume('foo1', {timeout: 2000}) YIELD event RETURN event")
        assertFalse { result.hasNext() }
    }

    @Test
    fun shouldReadArrayOfJson() {
        db = graphDatabaseBuilder.newGraphDatabase() as GraphDatabaseAPI
        db.dependencyResolver.resolveDependency(Procedures::class.java)
                .registerProcedure(StreamsSinkProcedures::class.java, true)
        val topic = "array-topic"
        val list = listOf(data, data)
        val producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(list))
        kafkaProducer.send(producerRecord).get()
        db.execute("""
            CALL streams.consume('$topic', {timeout: 5000}) YIELD event
            UNWIND event.data AS data
            CREATE (t:TEST) SET t += data.properties
        """.trimIndent()).close()
        val searchResult = db.execute("MATCH (t:TEST) WHERE properties(t) = {props} RETURN count(t) AS count", mapOf("props" to dataProperties))
        assertTrue { searchResult.hasNext() }
        val searchResultMap = searchResult.next()
        assertTrue { searchResultMap.containsKey("count") }
        assertEquals(2L, searchResultMap["count"])
    }

    @Test
    fun shouldReadSimpleDataType() {
        db = graphDatabaseBuilder.newGraphDatabase() as GraphDatabaseAPI
        db.dependencyResolver.resolveDependency(Procedures::class.java)
                .registerProcedure(StreamsSinkProcedures::class.java, true)
        val topic = "simple-data"
        val simpleInt = 1
        val simpleBoolean = true
        val simpleString = "test"
        var producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(simpleInt))
        kafkaProducer.send(producerRecord).get()
        producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(simpleBoolean))
        kafkaProducer.send(producerRecord).get()
        producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(simpleString))
        kafkaProducer.send(producerRecord).get()
        db.execute("""
            CALL streams.consume('$topic', {timeout: 5000}) YIELD event
            MERGE (t:LOG{simpleData: event.data})
            RETURN count(t) AS insert
        """.trimIndent()).close()
        val searchResult = db.execute("""
            MATCH (l:LOG)
            WHERE l.simpleData IN [$simpleInt, $simpleBoolean, "$simpleString"]
            RETURN count(l) as count
        """.trimIndent())
        assertTrue { searchResult.hasNext() }
        val searchResultMap = searchResult.next()
        assertTrue { searchResultMap.containsKey("count") }
        assertEquals(3L, searchResultMap["count"])

    }

    @Test
    fun shouldReadATopicPartitionStartingFromAnOffset() = runBlocking {
        db = graphDatabaseBuilder.newGraphDatabase() as GraphDatabaseAPI
        db.dependencyResolver.resolveDependency(Procedures::class.java)
                .registerProcedure(StreamsSinkProcedures::class.java, true)
        val topic = "read-from-range"
        val simpleInt = 1
        val partition = 0
        var start = -1L
        (1..10).forEach {
            val producerRecord = ProducerRecord(topic, partition, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(simpleInt))
            val recordMetadata = kafkaProducer.send(producerRecord).get()
            if (it == 6) {
                start = recordMetadata.offset()
            }
        }
        delay(1000)
        db.execute("""
            CALL streams.consume('$topic', {timeout: 5000, partitions: [{partition: $partition, offset: $start}]}) YIELD event
            CREATE (t:LOG{simpleData: event.data})
            RETURN count(t) AS insert
        """.trimIndent()).close()

        val count = db.execute("""
            MATCH (l:LOG)
            RETURN count(l) as count
        """.trimIndent()).columnAs<Long>("count").next()
        assertEquals(5L, count)
    }

    @Test
    fun shouldReadFromLatest() = runBlocking {
        db = graphDatabaseBuilder.newGraphDatabase() as GraphDatabaseAPI
        db.dependencyResolver.resolveDependency(Procedures::class.java)
                .registerProcedure(StreamsSinkProcedures::class.java, true)
        val topic = "simple-data-from-latest"
        val simpleInt = 1
        val simpleString = "test"
        val partition = 0
        (1..10).forEach {
            val producerRecord = ProducerRecord(topic, partition, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(simpleInt))
            kafkaProducer.send(producerRecord).get()
        }
        delay(1000) // should ignore the three above
        GlobalScope.launch(Dispatchers.IO) {
            delay(1000)
            val producerRecord = ProducerRecord(topic, partition, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(simpleString))
            kafkaProducer.send(producerRecord).get()
        }
        db.execute("""
            CALL streams.consume('$topic', {timeout: 5000, from: 'latest', groupId: 'foo'}) YIELD event
            CREATE (t:LOG{simpleData: event.data})
            RETURN count(t) AS insert
        """.trimIndent()).close()
        val searchResult = db.execute("""
            MATCH (l:LOG)
            RETURN count(l) AS count
        """.trimIndent())
        assertTrue { searchResult.hasNext() }
        val searchResultMap = searchResult.next()
        assertTrue { searchResultMap.containsKey("count") }
        assertEquals(1L, searchResultMap["count"])
    }

    @Test
    fun shouldNotCommit() {
        db = graphDatabaseBuilder.newGraphDatabase() as GraphDatabaseAPI
        db.dependencyResolver.resolveDependency(Procedures::class.java)
                .registerProcedure(StreamsSinkProcedures::class.java, true)
        val topic = "simple-data"
        val simpleInt = 1
        val partition = 0
        var producerRecord = ProducerRecord(topic, partition, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(simpleInt))
        kafkaProducer.send(producerRecord).get()
        db.execute("""
            CALL streams.consume('$topic', {timeout: 5000, autoCommit: false, commit:false}) YIELD event
            MERGE (t:LOG{simpleData: event.data})
            RETURN count(t) AS insert
        """.trimIndent()).close()
        val searchResult = db.execute("""
            MATCH (l:LOG)
            RETURN count(l) as count
        """.trimIndent())
        assertTrue { searchResult.hasNext() }
        val searchResultMap = searchResult.next()
        assertTrue { searchResultMap.containsKey("count") }
        assertEquals(1L, searchResultMap["count"])

        val kafkaConsumer = createConsumer<String, ByteArray>()
        val offsetAndMetadata = kafkaConsumer.committed(TopicPartition(topic, partition))
        assertNull(offsetAndMetadata)
    }
}