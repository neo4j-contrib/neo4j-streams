package integrations

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.*
import org.junit.rules.TestName
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.test.TestGraphDatabaseFactory
import org.testcontainers.containers.KafkaContainer
import streams.serialization.JSONUtils
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue


class KafkaEventSinkIT {
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
        @ClassRule
        @JvmField
        val kafka = KafkaContainer(confluentPlatformVersion)
    }

    private lateinit var db: GraphDatabaseAPI

    private val cypherQueryTemplate = "MERGE (n:Label {id: event.id}) ON CREATE SET n += event.properties"

    private val topics = listOf("shouldWriteCypherQuery")

    @Rule
    @JvmField
    var testName = TestName()

    private val EXCLUDE_LOAD_TOPIC_METHOD_SUFFIX = "WithNoTopicLoaded"
    private val BATCH_SIZE_METHOD_SUFFIX = "WithBatchSize"

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
        if (testName.methodName.endsWith(BATCH_SIZE_METHOD_SUFFIX)) {
            graphDatabaseBuilder.setConfig("streams.sink.batch.size", "5")
        } else {
            graphDatabaseBuilder.setConfig("streams.sink.batch.timeout", "100")
        }
        db = graphDatabaseBuilder.newGraphDatabase() as GraphDatabaseAPI

        kafkaProperties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafka.bootstrapServers
        kafkaProperties["zookeeper.connect"] = kafka.envMap["KAFKA_ZOOKEEPER_CONNECT"]
//        kafkaProperties["acks"] = "all"
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
    fun shouldWriteDataFromSink() = runBlocking {
        val producerRecord = ProducerRecord(topics[0], UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(data))
        kafkaProducer.send(producerRecord).get()
        delay(1000)
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
        val result = db.execute("MATCH (n:Label) WHERE properties(n) = {props} RETURN count(*) AS count", mapOf("props" to props))
                .columnAs<Long>("count")
        assertTrue { result.hasNext() }
        assertEquals(1L, result.next())
    }

    @Test
    fun shouldNotWriteDataFromSinkWithNoTopicLoaded() = runBlocking {
        val producerRecord = ProducerRecord(topics[0], UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(data))
        kafkaProducer.send(producerRecord).get()
        delay(1000)
        val result = db.execute("MATCH (n:Label) RETURN n")
        assertFalse { result.hasNext() }
    }

    @Test
    fun shouldWriteDataWithBatchSize() = runBlocking {
        // batchTimeout default (30000), batchSize 5
        val mutableData = data.toMutableMap()
        repeat(5) {
            mutableData["id"] = it
            kafkaProducer.send(ProducerRecord(topics[0], UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(mutableData))).get()
            if (it < 4) { // test the data written
                delay(1000)
                val result = db.execute("MATCH (n:Label) RETURN COUNT(n) AS count")
                val resultMap = result.next()
                assertTrue { resultMap.containsKey("count") }
                assertEquals(0L, resultMap["count"])
            }

        }
        delay(1000)
        // after the 5th event the batch size is satisfied so the data should be written
        val result = db.execute("MATCH (n:Label) WHERE n.prop1 = {prop1} AND n.bar = {bar} RETURN count(*) AS count", dataProperties)
                .columnAs<Long>("count")
        assertTrue { result.hasNext() }
        assertEquals(5L, result.next())
    }
}