package streams.integrations

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import org.neo4j.test.rule.DbmsRule
import org.neo4j.test.rule.ImpermanentDbmsRule
import streams.KafkaTestUtils
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