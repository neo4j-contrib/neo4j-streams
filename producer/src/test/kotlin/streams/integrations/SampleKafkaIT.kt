package streams.integrations

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.LongDeserializer
import org.junit.After
import org.junit.Before
import org.junit.ClassRule
import org.junit.Test
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.test.TestGraphDatabaseFactory
import org.testcontainers.containers.KafkaContainer
import streams.kafka.KafkaConfiguration
import streams.serialization.JacksonUtil
import kotlin.test.assertEquals

class SampleKafkaIT {

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
    }

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

    private fun createNode() {
        db!!.execute("CREATE (:Person {name:'John Doe', age:42})").close()
    }

    @Test
    fun testCreateNode() {
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)

        val props = config.asProperties()
        props.put("group.id", "neo4j")
        props.put("enable.auto.commit", "true");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer::class.java)
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer::class.java)
        val consumer = KafkaConsumer<Long, ByteArray>(props)
        consumer.subscribe(listOf("neo4j"))
        Thread {
            Thread.sleep(1000)
            createNode()
        }.start()
        val records = consumer.poll(5000)
        assertEquals(1, records.count())
        assertEquals(true, records.all {
            JacksonUtil.getMapper().readValue(it.value(), Map::class.java).let {
                val map = it["value"] as Map<String, Any>
                var payload = map["payload"] as Map<String, Any?>
                val after = payload["after"] as Map<String, Any?>
                val labels = after["labels"] as List<String>
                val propertiesAfter = after["properties"] as Map<String, Any?>
                val meta = map["meta"] as Map<String, Any?>
                labels == listOf("Person") && propertiesAfter == mapOf("name" to "John Doe", "age" to 42) && meta["operation"] == "created"
            }
        })
    }
}