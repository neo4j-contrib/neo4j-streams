package integrations.kafka

import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.neo4j.test.rule.DbmsRule
import org.neo4j.test.rule.ImpermanentDbmsRule
import streams.KafkaTestUtils
import streams.setConfig
import streams.shutdownSilently

open class KafkaEventSinkBaseTSE {

    companion object {

        private var startedFromSuite = true

        @BeforeClass
        @BeforeAll
        @JvmStatic
        fun setUpContainer() {
            if (!KafkaEventSinkSuiteIT.isRunning) {
                startedFromSuite = false
                KafkaEventSinkSuiteIT.setUpContainer()
            }
        }

        @AfterClass
        @AfterAll
        @JvmStatic
        fun tearDownContainer() {
            if (!startedFromSuite) {
                KafkaEventSinkSuiteIT.tearDownContainer()
            }
        }
    }

    lateinit var db: DbmsRule

    lateinit var kafkaProducer: KafkaProducer<String, ByteArray>
    lateinit var kafkaAvroProducer: KafkaProducer<GenericRecord, GenericRecord>

    val cypherQueryTemplate = "MERGE (n:Label {id: event.id}) ON CREATE SET n += event.properties"

    // Test data
    val dataProperties = mapOf("prop1" to "foo", "bar" to 1)
    val data = mapOf("id" to 1, "properties" to dataProperties)

    @Before
    @BeforeEach
    fun setUp() {
        db = ImpermanentDbmsRule()
                .setConfig("kafka.bootstrap.servers", KafkaEventSinkSuiteIT.kafka.bootstrapServers)
                .setConfig("streams.sink.enabled", "true")
        kafkaProducer = KafkaTestUtils.createProducer(
                bootstrapServers = KafkaEventSinkSuiteIT.kafka.bootstrapServers)
        kafkaAvroProducer = KafkaTestUtils.createProducer(
                bootstrapServers = KafkaEventSinkSuiteIT.kafka.bootstrapServers,
                schemaRegistryUrl = KafkaEventSinkSuiteIT.schemaRegistry.getSchemaRegistryUrl(),
                keySerializer = KafkaAvroSerializer::class.java.name,
                valueSerializer = KafkaAvroSerializer::class.java.name)
    }

    private fun <K, V> KafkaProducer<K, V>.flushAndClose() {
        this.flush()
        this.close()
    }

    @After
    @AfterEach
    fun tearDown() {
        if (::db.isInitialized) {
            db.shutdownSilently()
        }
        if (::kafkaProducer.isInitialized) {
            kafkaProducer.flushAndClose()
        }
        if (::kafkaAvroProducer.isInitialized) {
            kafkaAvroProducer.flushAndClose()
        }
    }
}