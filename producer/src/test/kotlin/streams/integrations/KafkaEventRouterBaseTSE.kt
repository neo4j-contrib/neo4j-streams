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
        fun isValidRelationship(event: StreamsTransactionEvent, type: OperationType) = when (type) {
            OperationType.created -> event.payload.before == null
                    && event.payload.after?.let { it.properties?.let { it.isNullOrEmpty() } } ?: false
                    && event.schema.properties == emptyMap<String, String>()
            OperationType.updated -> event.payload.before?.let { it.properties?.let { it.isNullOrEmpty() } } ?: false
                    && event.payload.after?.let { it.properties == mapOf("type" to "update") } ?: false
                    && event.schema.properties == mapOf("type" to "String")
            OperationType.deleted -> event.payload.before?.let { it.properties == mapOf("type" to "update") } ?: false
                    && event.payload.after == null
                    && event.schema.properties == mapOf("type" to "String")
            else -> throw IllegalArgumentException("Unsupported OperationType")
        }
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