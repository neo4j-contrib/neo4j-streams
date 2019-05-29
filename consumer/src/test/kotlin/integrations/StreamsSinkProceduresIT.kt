package integrations

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.*
import org.junit.rules.TestName
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.test.TestGraphDatabaseFactory
import org.testcontainers.containers.KafkaContainer
import streams.procedures.StreamsSinkProcedures
import streams.serialization.JSONUtils
import streams.utils.StreamsUtils
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@Suppress("UNCHECKED_CAST", "DEPRECATION")
class StreamsSinkProceduresIT {
    companion object {
        /**
         * Kafka TestContainers uses Confluent OSS images.
         * We need to keep in mind which is the right Confluent Platform version for the Kafka version this project uses
         *
         * Confluent Platform | Apache Kafka
         *                    |
         * 4.0.x	          | 1.0.x
         * 4.1.x	          | 1.1.x
         * 5.0.x	          | 2.0.x
         *
         * Please see also https://docs.confluent.io/current/installation/versions-interoperability.html#cp-and-apache-kafka-compatibility
         */
        private const val confluentPlatformVersion = "4.0.2"
        @JvmStatic
        lateinit var kafka: KafkaContainer

        @BeforeClass @JvmStatic
        fun setUpContainer() {
            var exists = false
            StreamsUtils.ignoreExceptions({
                kafka = KafkaContainer(confluentPlatformVersion)
                kafka.start()
                exists = true
            }, IllegalStateException::class.java)
            Assume.assumeTrue("Kafka container has to exist", exists)
            Assume.assumeTrue("Kafka must be running", kafka.isRunning)
        }

        @AfterClass @JvmStatic
        fun tearDownContainer() {
            StreamsUtils.ignoreExceptions({
                kafka.stop()
            }, UninitializedPropertyAccessException::class.java)
        }
    }

    private lateinit var db: GraphDatabaseAPI

    private val cypherQueryTemplate = "MERGE (n:Label {id: event.id}) ON CREATE SET n += event.properties"

    @Rule
    @JvmField
    var testName = TestName()

    private val EXCLUDE_LOAD_TOPIC_METHOD_SUFFIX = "WithNoTopicLoaded"
    private val EXCLUDE_SINK_METHOD_SUFFIX = "WithSinkDisabled"

    private val kafkaProperties = Properties()

    private lateinit var kafkaProducer: KafkaProducer<String, ByteArray>

    // Test data
    private val dataProperties = mapOf("prop1" to "foo", "bar" to 1)
    private val data = mapOf("id" to 1, "properties" to dataProperties)


    @Before
    fun setUp() {
        var graphDatabaseBuilder = TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig("kafka.bootstrap.servers", kafka.bootstrapServers)
        if (!testName.methodName.endsWith(EXCLUDE_LOAD_TOPIC_METHOD_SUFFIX)) {
            graphDatabaseBuilder.setConfig("streams.sink.topic.cypher.shouldWriteCypherQuery", cypherQueryTemplate)
        }
        if (testName.methodName.endsWith(EXCLUDE_SINK_METHOD_SUFFIX)) {
            graphDatabaseBuilder.setConfig("streams.sink.enabled", "false")
        }
        db = graphDatabaseBuilder.newGraphDatabase() as GraphDatabaseAPI
        db.dependencyResolver.resolveDependency(Procedures::class.java)
                .registerProcedure(StreamsSinkProcedures::class.java, true)

        kafkaProperties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafka.bootstrapServers
        kafkaProperties["zookeeper.connect"] = kafka.envMap["KAFKA_ZOOKEEPER_CONNECT"]
        kafkaProperties["group.id"] = "neo4j"
        kafkaProperties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        kafkaProperties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = ByteArraySerializer::class.java

        kafkaProducer = KafkaProducer(kafkaProperties)
    }

    @After
    fun tearDown() {
        db.shutdown()
        kafkaProducer.close()
    }


    @Test
    fun shouldConsumeDataFromProcedureWithSinkDisabled() {
        val topic = "bar"
        testProcedure(topic)
    }

    @Test
    fun shouldConsumeDataFromProcedure() {
        val topic = "foo"
        testProcedure(topic)
    }

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
    fun shouldTimeout() {
        val result = db.execute("CALL streams.consume('foo1', {timeout: 2000}) YIELD event RETURN event")
        assertFalse { result.hasNext() }
    }

    @Test
    fun shouldReadArrayOfJson() {
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

}