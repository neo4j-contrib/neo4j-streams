package streams.integrations

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.After
import org.junit.Before
import org.junit.ClassRule
import org.junit.Test
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.test.TestGraphDatabaseFactory
import org.testcontainers.containers.KafkaContainer
import streams.kafka.KafkaConfiguration
import streams.procedures.StreamsProcedures
import streams.serialization.JSONUtils
import kotlin.test.assertEquals

class KafkaEventRouterIT {

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

    lateinit var db: GraphDatabaseAPI

    @Before
    fun setUp() {
        db = TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig("kafka.bootstrap.servers", kafka.bootstrapServers)
                .newGraphDatabase() as GraphDatabaseAPI
        db.dependencyResolver.resolveDependency(Procedures::class.java)
                .registerProcedure(StreamsProcedures::class.java, true)
    }

    @After
    fun tearDown() {
        db.shutdown()
    }

    @Test
    fun testCreateNode() {
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        val consumer = createConsumer(config)
        consumer.subscribe(listOf("neo4j"))
        db.execute("CREATE (:Person {name:'John Doe', age:42})").close()
        val records = consumer.poll(5000)
        assertEquals(1, records.count())
        assertEquals(true, records.all {
            JSONUtils.readValue(it.value(), Map::class.java).let {
                var payload = it["payload"] as Map<String, Any?>
                val after = payload["after"] as Map<String, Any?>
                val labels = after["labels"] as List<String>
                val propertiesAfter = after["properties"] as Map<String, Any?>
                val meta = it["meta"] as Map<String, Any?>
                labels == listOf("Person") && propertiesAfter == mapOf("name" to "John Doe", "age" to 42) && meta["operation"] == "created"
            }
        })
        consumer.close()
    }

    private fun createConsumer(config: KafkaConfiguration): KafkaConsumer<String, ByteArray> {
        val props = config.asProperties()
        props["group.id"] = "neo4j"
        props["enable.auto.commit"] = "true"
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = ByteArrayDeserializer::class.java
        props["auto.offset.reset"] = "earliest"
        return KafkaConsumer(props)
    }

    @Test
    fun testProcedure() {
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        val consumer = createConsumer(config)
        consumer.subscribe(listOf("neo4j"))
        val message = "Hello World"
        db.execute("CALL streams.publish('neo4j', '$message')").close()
        val records = consumer.poll(5000)
        assertEquals(1, records.count())
        assertEquals(true, records.all {
            JSONUtils.readValue(it.value(), Map::class.java).let {
                val payload = it as Map<String, String>
                message == payload["payload"]
            }
        })
        consumer.close()
    }

}