package kafka

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.codehaus.jackson.map.ObjectMapper
import org.junit.After
import org.junit.Before
import org.junit.ClassRule
import org.junit.Test
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.test.TestGraphDatabaseFactory
import org.testcontainers.containers.KafkaContainer
import kotlin.test.assertEquals

class SampleKafkaIntegrationTest {

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
        @ClassRule @JvmField
        val kafka = KafkaContainer(confluentPlatformVersion)

        // This can be static
        val mapper = ObjectMapper()
    }

    // TODO: maybe for a "full" integration test we can also use a Neo4j container
    var db: GraphDatabaseService? = null

    @Before
    fun setUp() {
        db = TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig("kafka.bootstrap.servers", kafka.bootstrapServers)
                .newGraphDatabase()
    }

    @After
    fun tearDown() {
        db?.shutdown()
    }

    @Test
    fun createNodes() {
        val config = KafkaConfiguration(kafkaHosts = kafka.bootstrapServers)

        val props = config.asProperties()
        props.put("enable.auto.commit", "true");
        val consumer = KafkaConsumer<Long, ByteArray>(props)
        consumer.subscribe(config.topics)
        Thread {
            db!!.execute("CREATE (:Person {name:'John Doe', age:42})").close()
        }.start()
        val records = consumer.poll(5000)
        records.forEach { println("offset = ${it.offset()}, key = ${it.key()}, value = ${mapper.readValue(it.value(), Object::class.java)}") }
        assertEquals(1, records.count())
        assertEquals(true, records.all {
            mapper.readValue(it.value(), Map::class.java).let {
                it["labels"] == listOf("Person") && it["data"] == mapOf("name" to "John Doe", "age" to 42) && it["state"] == "created"
            }
        })
    }
}