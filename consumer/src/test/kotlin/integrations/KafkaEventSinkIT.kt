package integrations

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.codehaus.jackson.map.ObjectMapper
import org.junit.*
import org.junit.rules.TestName
import org.neo4j.graphdb.Node
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.test.TestGraphDatabaseFactory
import org.testcontainers.containers.KafkaContainer
import java.util.*
import java.util.concurrent.ThreadLocalRandom
import kotlin.test.assertEquals


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

    private val objectMapper: ObjectMapper = ObjectMapper()

    private val cypherQueryTemplate = "MERGE (n:Label {id: event.id}) ON CREATE SET n += event.properties"

    private val topics = listOf("shouldWriteCypherQuery")

    @Rule
    @JvmField
    var testName = TestName()

    private val EXCLUDE_LOAD_TOPIC_METHOD_SUFFIX = "WithNoTopicLoaded"

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
        db = graphDatabaseBuilder.newGraphDatabase() as GraphDatabaseAPI

        kafkaProperties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafka.bootstrapServers
        kafkaProperties["group.id"] = "neo4j"
        kafkaProperties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        kafkaProperties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = ByteArraySerializer::class.java

        AdminClient.create(kafkaProperties).use { client -> client.createTopics(
                topics.map { topic -> NewTopic(topic, 1, 1.toShort()) }) }
        kafkaProducer = KafkaProducer(kafkaProperties)
    }

    @After
    fun tearDown() {
        db.shutdown()
    }

    @Test
    fun shouldWriteDataFromSink() = runBlocking {
        val job = GlobalScope.launch {
            var partition = ThreadLocalRandom.current().nextInt(1)
            var producerRecord = ProducerRecord(topics[0], partition, System.currentTimeMillis(),
                    UUID.randomUUID().toString(),
                    objectMapper.writeValueAsBytes(data))
            kafkaProducer.send(producerRecord).get()
            delay(5000)
        }
        job.join()
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
        db.beginTx().use {
            db.execute("MATCH (n:Label) WHERE properties(n) = {props} RETURN count(*) AS count", mapOf("props" to props))
                    .columnAs<Long>("count").use {
                        assertEquals(true, it.hasNext())
                        val count = it.next()
                        assertEquals(1, count)
                        assertEquals(false, it.hasNext())
                    }
            it.success()
        }
    }

    @Test
    fun shouldNotWriteDataFromSinkWithNoTopicLoaded() = runBlocking {
        val job = GlobalScope.launch {
            var partition = ThreadLocalRandom.current().nextInt(1)
            var producerRecord = ProducerRecord(topics[0], partition, System.currentTimeMillis(),
                    UUID.randomUUID().toString(),
                    objectMapper.writeValueAsBytes(data))
            kafkaProducer.send(producerRecord).get()
            delay(5000)
        }
        job.join()
        db.beginTx().use {
            db.execute("MATCH (n:Label) RETURN n")
                    .columnAs<Node>("n").use {
                        assertEquals(false, it.hasNext()) // No records
                    }
            it.success()
        }
    }

}