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

open class KafkaEventRouterLogCompactionBaseTSE { // TSE (Test Suite Element)

    companion object {

        private var startedFromSuite = true

        @BeforeClass
        @JvmStatic
        fun setUpContainer() {
            if (!KafkaEventRouterLogCompactionSuiteIT.isRunning) {
                startedFromSuite = false
                KafkaEventRouterLogCompactionSuiteIT.setUpContainer()
            }
        }

        @AfterClass
        @JvmStatic
        fun tearDownContainer() {
            if (!startedFromSuite) {
                KafkaEventRouterLogCompactionSuiteIT.tearDownContainer()
            }
        }
    }

    val db: DbmsRule = ImpermanentDbmsRule()

    lateinit var kafkaConsumer: KafkaConsumer<String, ByteArray>

    @Before
    fun setUp() {
        kafkaConsumer = KafkaTestUtils.createConsumer(bootstrapServers = KafkaEventRouterLogCompactionSuiteIT.kafka.bootstrapServers)
        db.setConfig("kafka.bootstrap.servers", KafkaEventRouterLogCompactionSuiteIT.kafka.bootstrapServers)
    }

    @After
    fun tearDown() {
        db.shutdown()
        kafkaConsumer.close()
    }
}