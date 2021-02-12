package streams

import org.junit.After
import org.junit.Before
import org.junit.Test
import org.mockito.Mockito
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.NullLog
import streams.kafka.KafkaSinkConfiguration
import streams.serialization.JSONUtils
import streams.service.Topics
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class StreamsSinkConfigurationListenerTest {
    private lateinit var db: GraphDatabaseAPI
    private lateinit var streamsSinkConfigurationListener: StreamsSinkConfigurationListener

    private val kafkaConfig = mapOf("streams.sink.topic.cypher.my-topic" to "CREATE (n:Label)")

    @Before
    fun setUp() {
        db = Mockito.mock(GraphDatabaseAPI::class.java)
        streamsSinkConfigurationListener = StreamsSinkConfigurationListener(db, NullLog.getInstance())
        val field = StreamsSinkConfigurationListener::class.java.getDeclaredField("lastConfig")
        field.isAccessible = true
        field.set(streamsSinkConfigurationListener, KafkaSinkConfiguration.create(kafkaConfig))
    }

    @After
    fun tearDown() {
        db.shutdown()
    }

    @Test
    fun `test configuration changes`() {
        val autoOffset = kafkaConfig + mapOf("kafka.auto.offset.reset" to "latest")
        assertTrue { streamsSinkConfigurationListener.isConfigurationChanged(autoOffset) }
        val sourceEnabled = kafkaConfig + mapOf("streams.source.enabled" to "true")
        assertFalse { streamsSinkConfigurationListener.isConfigurationChanged(sourceEnabled) }
        val transactionalId = kafkaConfig + mapOf("kafka.transactional.id" to "foo")
        assertFalse { streamsSinkConfigurationListener.isConfigurationChanged(transactionalId) }
    }

}