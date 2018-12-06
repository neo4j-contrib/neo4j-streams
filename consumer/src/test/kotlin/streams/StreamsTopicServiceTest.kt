package streams

import org.junit.After
import org.junit.Before
import org.junit.Test
import org.neo4j.kernel.impl.core.EmbeddedProxySPI
import org.neo4j.kernel.impl.core.GraphProperties
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.test.TestGraphDatabaseFactory
import streams.kafka.KafkaSinkConfiguration
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class StreamsTopicServiceTest {

    private lateinit var db: GraphDatabaseAPI
    private lateinit var streamsTopicService: StreamsTopicService
    private lateinit var kafkaConfig: KafkaSinkConfiguration
    private lateinit var graphProperties: GraphProperties

    @Before
    fun setUp() {
        db = TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .newGraphDatabase() as GraphDatabaseAPI
        kafkaConfig = KafkaSinkConfiguration(streamsSinkConfiguration = StreamsSinkConfiguration(topics = mapOf("shouldWriteCypherQuery" to "MERGE (n:Label {id: event.id})\n" +
                "    ON CREATE SET n += event.properties")))
        streamsTopicService = StreamsTopicService(db, kafkaConfig.streamsSinkConfiguration.topics)
        graphProperties = db.dependencyResolver.resolveDependency(EmbeddedProxySPI::class.java).newGraphPropertiesProxy()
    }

    @After
    fun tearDown() {
        db.shutdown()
    }

    private fun assertProperty(entry: Map.Entry<String, String>) {
        assertEquals(entry.value, streamsTopicService.get(entry.key))
        db.beginTx().use { assertTrue { graphProperties.hasProperty("streams.sink.topic.${entry.key}") } }

    }

    @Test
    fun shouldStoreTopicAndCypherTemplate() {
        kafkaConfig.streamsSinkConfiguration.topics.forEach { assertProperty(it) }
    }

    @Test
    fun shouldStoreTopicsAndCypherTemplate() {
        val map = mapOf("topic1" to "MERGE (n:Label1 {id: event.id})",
                "topic2" to "MERGE (n:Label2 {id: event.id})")
        streamsTopicService.setAll(map)

        val allTopics = map.plus(kafkaConfig.streamsSinkConfiguration.topics)
        allTopics.forEach { assertProperty(it) }

        assertEquals(allTopics, streamsTopicService.getAll())

        assertEquals(allTopics.keys, streamsTopicService.getTopics())

    }
}