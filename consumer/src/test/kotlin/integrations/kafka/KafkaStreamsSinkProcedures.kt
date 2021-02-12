package integrations.kafka

import integrations.kafka.KafkaTestUtils.createConsumer
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import kotlinx.coroutines.*
import extension.newDatabase
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.junit.Test
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.kernel.internal.GraphDatabaseAPI
import streams.events.StreamsPluginStatus
import streams.extensions.toMap
import streams.procedures.StreamsSinkProcedures
import streams.serialization.JSONUtils
import java.util.*
import java.util.stream.Collectors
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
        db = graphDatabaseBuilder.newDatabase(StreamsPluginStatus.STOPPED) as GraphDatabaseAPI
        db.dependencyResolver.resolveDependency(Procedures::class.java)
                .registerProcedure(StreamsSinkProcedures::class.java, true)
        val topic = "bar"
        testProcedure(topic)
    }

    @Test
    fun shouldConsumeDataFromProcedure() {
        db = graphDatabaseBuilder.newDatabase(StreamsPluginStatus.STOPPED) as GraphDatabaseAPI
        db.dependencyResolver.resolveDependency(Procedures::class.java)
                .registerProcedure(StreamsSinkProcedures::class.java, true)
        val topic = "foo"
        testProcedure(topic)
    }

    @Test
    fun shouldTimeout() {
        db = graphDatabaseBuilder.newDatabase(StreamsPluginStatus.STOPPED) as GraphDatabaseAPI
        db.dependencyResolver.resolveDependency(Procedures::class.java)
                .registerProcedure(StreamsSinkProcedures::class.java, true)
        val result = db.execute("CALL streams.consume('foo1', {timeout: 2000}) YIELD event RETURN event")
        assertFalse { result.hasNext() }
    }

    @Test
    fun shouldReadArrayOfJson() {
        db = graphDatabaseBuilder.newDatabase(StreamsPluginStatus.STOPPED) as GraphDatabaseAPI
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
        db = graphDatabaseBuilder.newDatabase(StreamsPluginStatus.STOPPED) as GraphDatabaseAPI
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
        db = graphDatabaseBuilder.newDatabase(StreamsPluginStatus.STOPPED) as GraphDatabaseAPI
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
        db = graphDatabaseBuilder.newDatabase(StreamsPluginStatus.STOPPED) as GraphDatabaseAPI
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
        db = graphDatabaseBuilder.newDatabase(StreamsPluginStatus.STOPPED) as GraphDatabaseAPI
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

        val kafkaConsumer = createConsumer<ByteArray, ByteArray>(
                kafka = KafkaEventSinkSuiteIT.kafka,
                schemaRegistry = KafkaEventSinkSuiteIT.schemaRegistry)
        val offsetAndMetadata = kafkaConsumer.committed(TopicPartition(topic, partition))
        assertNull(offsetAndMetadata)
    }

    @Test
    fun `should consume AVRO messages`() {
        db = graphDatabaseBuilder.newDatabase(StreamsPluginStatus.STOPPED) as GraphDatabaseAPI
        db.dependencyResolver.resolveDependency(Procedures::class.java)
                .registerProcedure(StreamsSinkProcedures::class.java, true)
        val PLACE_SCHEMA = SchemaBuilder.builder("com.namespace")
                .record("Place").fields()
                .name("name").type().stringType().noDefault()
                .name("coordinates").type().array().items().doubleType().noDefault()
                .name("citizens").type().longType().noDefault()
                .endRecord()
        val coordinates = listOf(42.30000, -11.22222)
        val citizens = 1_000_000L
        val struct = GenericRecordBuilder(PLACE_SCHEMA)
                .set("name", "Foo")
                .set("coordinates", coordinates)
                .set("citizens", citizens)
                .build()
        val topic = "avro-procedure"
        val keyDeserializer = KafkaAvroDeserializer::class.java.name
        val valueDeserializer = KafkaAvroDeserializer::class.java.name
        kafkaAvroProducer.send(ProducerRecord(topic, null, struct)).get()
        val schemaRegistryUrl = KafkaEventSinkSuiteIT.schemaRegistry.getSchemaRegistryUrl()
        val result = db.execute("""
            CALL streams.consume('$topic', {timeout: 5000, keyDeserializer: '$keyDeserializer', valueDeserializer: '$valueDeserializer', schemaRegistryUrl: '$schemaRegistryUrl'}) YIELD event
            RETURN event
        """.trimIndent())
        assertTrue { result.hasNext() }
        val resultMap = result.next()
        assertTrue { resultMap.containsKey("event") }
        assertNotNull(resultMap["event"], "should contain event")
        val event = resultMap["event"] as Map<String, Any?>
        val resultData = event["data"] as Map<String, Any?>
        assertEquals(struct.toMap(), resultData)
    }

    @Test
    fun `should report the streams sink config`() {
        // given
        db = graphDatabaseBuilder.newDatabase(StreamsPluginStatus.STOPPED) as GraphDatabaseAPI
        db.dependencyResolver.resolveDependency(Procedures::class.java)
                .registerProcedure(StreamsSinkProcedures::class.java, true)
        val expected = mapOf("invalid_topics" to emptyList<String>(),
                "streams.sink.topic.pattern.relationship" to emptyMap<String, Any>(),
                "streams.sink.topic.cud" to emptySet<String>(),
                "streams.sink.topic.cdc.sourceId" to emptySet<String>(),
                "streams.sink.topic.cypher" to emptyMap<String, Any>(),
                "streams.sink.topic.cdc.schema" to emptySet<String>(),
                "streams.sink.topic.pattern.node" to emptyMap<String, Any>(),
                "streams.sink.errors" to emptyMap<String, Any>(),
                "streams.cluster.only" to false,
                "streams.sink.poll.interval" to 0L,
                "streams.sink.source.id.strategy.config" to mapOf("labelName" to "SourceEvent", "idName" to "sourceId"))

        // when
        val result = db.execute("CALL streams.sink.config()")

        // then
        val actual = result.stream()
                .collect(Collectors.toList())
                .map { it.getValue("name").toString() to it.getValue("value") }
                .toMap()
        assertEquals(expected, actual)
    }

    @Test
    fun `should report the streams sink status RUNNING`() = runBlocking {
        // given
        graphDatabaseBuilder.setConfig("streams.sink.topic.cypher.shouldWriteCypherQuery", cypherQueryTemplate)
        db = graphDatabaseBuilder.newDatabase() as GraphDatabaseAPI
        db.dependencyResolver.resolveDependency(Procedures::class.java)
                .registerProcedure(StreamsSinkProcedures::class.java, true)
        val expectedRunning = listOf(mapOf("name" to "status", "value" to StreamsPluginStatus.RUNNING.toString()))

        // when
        val result = db.execute("CALL streams.sink.status()")

        // then
        val actual = result.stream()
                .collect(Collectors.toList())
        assertEquals(expectedRunning, actual)
    }

    @Test
    fun `should report the streams sink status STOPPED`() {
        // given
        graphDatabaseBuilder.setConfig("streams.sink.topic.cypher.shouldWriteCypherQuery", cypherQueryTemplate)
        db = graphDatabaseBuilder.newDatabase() as GraphDatabaseAPI
        db.dependencyResolver.resolveDependency(Procedures::class.java)
                .registerProcedure(StreamsSinkProcedures::class.java, true)
        val expectedRunning = listOf(mapOf("name" to "status", "value" to StreamsPluginStatus.STOPPED.toString()))
        db.execute("CALL streams.sink.stop()")

        // when
        val result = db.execute("CALL streams.sink.status()")

        // then
        val actual = result.stream()
                .collect(Collectors.toList())
        assertEquals(expectedRunning, actual)
    }
}