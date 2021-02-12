package streams

import org.junit.After
import org.junit.Before
import org.junit.Test
import org.mockito.Mockito
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.NullLog
import streams.kafka.KafkaConfiguration
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class StreamsRouterConfigurationListenerTest {
    private lateinit var db: GraphDatabaseAPI
    private lateinit var streamsRouterConfigurationListener: StreamsRouterConfigurationListener

    private val kafkaConfig = mapOf("kafka.zookeeper.connect" to "zookeeper:1234",
        "kafka.bootstrap.servers" to "kafka:5678",
        "kafka.acks" to "10",
        "kafka.num.partitions" to "1",
        "kafka.retries" to "1",
        "kafka.batch.size" to "10",
        "kafka.buffer.memory" to "1000",
        "kafka.reindex.batch.size" to "1",
        "kafka.session.timeout.ms" to "1",
        "kafka.connection.timeout.ms" to "1",
        "kafka.replication" to "2",
        "kafka.linger.ms" to "10",
        "kafka.fetch.min.bytes" to "1234",
        "kafka.topic.discovery.polling.interval" to "0")

    @Before
    fun setUp() {
        db = Mockito.mock(GraphDatabaseAPI::class.java)
        streamsRouterConfigurationListener = StreamsRouterConfigurationListener(db, NullLog.getInstance())
        val lastConfig = StreamsRouterConfigurationListener::class.java.getDeclaredField("lastConfig")
        lastConfig.isAccessible = true
        lastConfig.set(streamsRouterConfigurationListener, KafkaConfiguration.create(kafkaConfig))
        val streamsEventRouterConfiguration = StreamsRouterConfigurationListener::class.java.getDeclaredField("streamsEventRouterConfiguration")
        streamsEventRouterConfiguration.isAccessible = true
        streamsEventRouterConfiguration.set(streamsRouterConfigurationListener, StreamsEventRouterConfiguration.from(kafkaConfig))
    }

    @After
    fun tearDown() {
        db.shutdown()
    }

    @Test
    fun `test configuration changes`() {
        val transactionalId = kafkaConfig + mapOf("kafka.transactional.id" to "foo")
        assertTrue { streamsRouterConfigurationListener.isConfigurationChanged(transactionalId) }
        val sourceCypherTopic = kafkaConfig + mapOf("streams.sink.topic.cypher.my-topic" to "CREATE (n:Label)")
        assertFalse { streamsRouterConfigurationListener.isConfigurationChanged(sourceCypherTopic) }
        val autoOffset = kafkaConfig + mapOf("kafka.auto.offset.reset" to "latest")
        assertFalse { streamsRouterConfigurationListener.isConfigurationChanged(autoOffset) }
        val clusterOnly = kafkaConfig + mapOf("streams.cluster.only" to "true")
        assertFalse { streamsRouterConfigurationListener.isConfigurationChanged(clusterOnly) }
    }

}