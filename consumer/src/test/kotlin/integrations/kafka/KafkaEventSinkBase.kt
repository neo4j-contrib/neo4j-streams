package integrations.kafka

import integrations.kafka.KafkaTestUtils.createProducer
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import org.neo4j.graphdb.factory.GraphDatabaseBuilder
import org.neo4j.kernel.internal.GraphDatabaseAPI
import java.util.*

open class KafkaEventSinkBase {

    companion object {

        private var startedFromSuite = true

        @BeforeClass
        @JvmStatic
        fun setUpContainer() {
            if (!KafkaEventSinkSuiteIT.isRunning) {
                startedFromSuite = false
                KafkaEventSinkSuiteIT.setUpContainer()
            }
        }

        @AfterClass
        @JvmStatic
        fun tearDownContainer() {
            if (!startedFromSuite) {
                KafkaEventSinkSuiteIT.tearDownContainer()
            }
        }
    }

    lateinit var graphDatabaseBuilder: GraphDatabaseBuilder

    lateinit var db: GraphDatabaseAPI

    lateinit var kafkaProducer: KafkaProducer<String, ByteArray>
    lateinit var kafkaAvroProducer: KafkaProducer<GenericRecord, GenericRecord>

    val cypherQueryTemplate = "MERGE (n:Label {id: event.id}) ON CREATE SET n += event.properties"

    // Test data
    val dataProperties = mapOf("prop1" to "foo", "bar" to 1)
    val data = mapOf("id" to 1, "properties" to dataProperties)

    @Before
    fun setUp() {
        graphDatabaseBuilder = org.neo4j.test.TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig("kafka.bootstrap.servers", KafkaEventSinkSuiteIT.kafka.bootstrapServers)
                .setConfig("kafka.zookeeper.connect", KafkaEventSinkSuiteIT.kafka.envMap["KAFKA_ZOOKEEPER_CONNECT"])
                .setConfig("streams.sink.enabled", "true")
        kafkaProducer = createProducer(kafka = KafkaEventSinkSuiteIT.kafka, schemaRegistry = KafkaEventSinkSuiteIT.schemaRegistry)
        kafkaAvroProducer = createProducer(
                kafka = KafkaEventSinkSuiteIT.kafka,
                schemaRegistry = KafkaEventSinkSuiteIT.schemaRegistry,
                valueSerializer = KafkaAvroSerializer::class.java.name,
                keySerializer = KafkaAvroSerializer::class.java.name)
    }

    @After
    fun tearDown() {
        if (::db.isInitialized) {
            db.shutdown()
        }
        if (::kafkaProducer.isInitialized) {
            kafkaProducer.close()
        }
        if (::kafkaAvroProducer.isInitialized) {
            kafkaAvroProducer.close()
        }
    }
}