package streams.integrations

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import org.neo4j.test.rule.DbmsRule
import org.neo4j.test.rule.ImpermanentDbmsRule
import streams.KafkaTestUtils
import streams.events.OperationType
import streams.events.StreamsTransactionEvent
import streams.setConfig

open class KafkaEventRouterBaseTSE { // TSE (Test Suit Element)

    companion object {

        private var startedFromSuite = true

        @BeforeClass
        @JvmStatic
        fun setUpContainer() {
            if (!KafkaEventRouterSuiteIT.isRunning) {
                startedFromSuite = false
                KafkaEventRouterSuiteIT.setUpContainer()
            }
        }

        @AfterClass
        @JvmStatic
        fun tearDownContainer() {
            if (!startedFromSuite) {
                KafkaEventRouterSuiteIT.tearDownContainer()
            }
        }

        // common methods
        fun commonRelAssertions(value: StreamsTransactionEvent) = value.meta.operation == OperationType.created
                && value.payload.before == null
                && value.payload.after?.let { it.properties?.let { it.isNullOrEmpty() } } ?: false
                && value.schema.properties == emptyMap<String, String>()

        fun commonRelAssertionsUpdate(value: StreamsTransactionEvent) = value.meta.operation == OperationType.updated
                && value.payload.before?.let { it.properties?.let { it.isNullOrEmpty() } } ?: false
                && value.payload.after?.let { it.properties == mapOf("type" to "update") } ?: false
                && value.schema.properties == mapOf("type" to "String")

        fun commonRelAssertionsDelete(value: StreamsTransactionEvent) = value.meta.operation == OperationType.deleted
                && value.payload.before?.let { it.properties == mapOf("type" to "update") } ?: false
                && value.payload.after == null
                && value.schema.properties == mapOf("type" to "String")
    }

    val db: DbmsRule = ImpermanentDbmsRule()

    lateinit var kafkaConsumer: KafkaConsumer<String, ByteArray>

    @Before
    fun setUp() {
        kafkaConsumer = KafkaTestUtils.createConsumer(bootstrapServers = KafkaEventRouterSuiteIT.kafka.bootstrapServers)
        db.setConfig("kafka.bootstrap.servers", KafkaEventRouterSuiteIT.kafka.bootstrapServers)
    }

    @After
    fun tearDown() {
        db.shutdown()
        kafkaConsumer.close()
    }
}