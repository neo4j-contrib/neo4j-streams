package streams

import org.junit.Before
import org.junit.Test
import org.mockito.Mockito
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.ResourceIterator
import org.neo4j.graphdb.Result
import org.neo4j.graphdb.Transaction
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.NullLog
import streams.config.StreamsConfig
import streams.service.TopicType
import streams.service.TopicValidationException
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class StreamsSinkConfigurationTest {

    lateinit var streamsConfig: StreamsConfig
    val defalutDbName = "neo4j"

    @Before
    fun setUp() {
        val dbms = Mockito.mock(DatabaseManagementService::class.java)
        val db = Mockito.mock(GraphDatabaseAPI::class.java)
        val tx = Mockito.mock(Transaction::class.java)
        val result = Mockito.mock(Result::class.java)
        val resource = Mockito.mock(ResourceIterator::class.java) as ResourceIterator<String>
        Mockito.`when`(dbms.database(Mockito.anyString())).thenReturn(db)
        Mockito.`when`(db.beginTx()).thenReturn(tx)
        Mockito.`when`(tx.execute(Mockito.anyString())).thenReturn(result)
        Mockito.`when`(result.columnAs<String>(Mockito.anyString())).thenReturn(resource)
        Mockito.`when`(resource.hasNext()).thenReturn(true)
        Mockito.`when`(resource.next()).thenReturn(defalutDbName)
        streamsConfig = StreamsConfig(NullLog.getInstance(), dbms)
    }

    @Test
    fun `should manage only topics for default db`() {
        val topicKey = "streams.sink.topic.cypher.myTopic"
        val topicValue = "MERGE (n:Label{ id: event.id })"
        val topicKeyNeo = "streams.sink.topic.cypher.myTopicNeo.to.neo4j"
        val topicValueNeo = "MERGE (n:Neo4j{ id: event.id })"
        val topicKeyFoo = "streams.sink.topic.cypher.myTopicFoo.to.foo"
        val topicValueFoo = "MERGE (n:Foo{ id: event.id })"
        val config = mapOf(topicKey to topicValue,
                topicKeyNeo to topicValueNeo,
                topicKeyFoo to topicValueFoo)
        streamsConfig.config.putAll(config)
        val streamsSinkConf = StreamsSinkConfiguration.from(streamsConfig, defalutDbName)
        val cypherTopics = streamsSinkConf.topics.asMap()[TopicType.CYPHER] as Map<String, String>
        assertEquals(mapOf("myTopic" to topicValue, "myTopicNeo" to topicValueNeo), cypherTopics)
    }

    @Test
    fun `should manage only topics for non default db`() {
        val topicKey = "streams.sink.topic.cypher.myTopic"
        val topicValue = "MERGE (n:Label{ id: event.id })"
        val topicKeyNeo = "streams.sink.topic.cypher.myTopicNeo.to.neo4j"
        val topicValueNeo = "MERGE (n:Neo4j{ id: event.id })"
        val topicKeyFoo = "streams.sink.topic.cypher.myTopicFoo.to.foo"
        val topicValueFoo = "MERGE (n:Foo{ id: event.id })"
        val config = mapOf(topicKey to topicValue,
                topicKeyNeo to topicValueNeo,
                topicKeyFoo to topicValueFoo)
        streamsConfig.config.putAll(config)
        val streamsSinkConf = StreamsSinkConfiguration.from(streamsConfig, "foo")
        val cypherTopics = streamsSinkConf.topics.asMap()[TopicType.CYPHER] as Map<String, String>
        assertEquals(mapOf("myTopicFoo" to topicValueFoo), cypherTopics)
    }

    @Test
    fun shouldReturnDefaultConfiguration() {
        val default = StreamsSinkConfiguration()
        testDefaultConf(default)
    }

    @Test
    fun shouldReturnConfigurationFromMap() {
        val topic = "topic-neo"
        val cdctopic = "cdctopic"
        val topicKey = "streams.sink.topic.cypher.$topic"
        val topicValue = "MERGE (n:Label{ id: event.id }) "
        val customLabel = "CustomLabel"
        val customId = "customId"
        val config = mapOf(topicKey to topicValue,
                "streams.sink.enabled" to "false",
                "streams.sink.topic.cdc.sourceId" to cdctopic,
                "streams.sink.topic.cdc.sourceId.labelName" to customLabel,
                "streams.sink.topic.cdc.sourceId.idName" to customId)
        streamsConfig.config.putAll(config)
        val streamsSinkConf = StreamsSinkConfiguration.from(streamsConfig, defalutDbName)
        testFromConf(streamsSinkConf, topic, topicValue)
        assertFalse { streamsSinkConf.enabled }
        assertEquals(setOf(cdctopic), streamsSinkConf.topics.asMap()[TopicType.CDC_SOURCE_ID])
        assertEquals(customLabel, streamsSinkConf.sourceIdStrategyConfig.labelName)
        assertEquals(customId, streamsSinkConf.sourceIdStrategyConfig.idName)
    }

    @Test(expected = TopicValidationException::class)
    fun shouldFailWithCrossDefinedTopics() {
        val topic = "topic-neo"
        val topicKey = "streams.sink.topic.cypher.$topic"
        val topicValue = "MERGE (n:Label{ id: event.id }) "
        val config = mapOf(topicKey to topicValue,
                "streams.sink.topic.pattern.node.nodePatternTopic" to "User{!userId,name,surname,address.city}",
                "streams.sink.enabled" to "false",
                "streams.sink.topic.cdc.sourceId" to topic)
        streamsConfig.config.putAll(config)
        StreamsSinkConfiguration.from(streamsConfig, defalutDbName)
    }

    @Test(expected = TopicValidationException::class)
    fun shouldFailWithCrossDefinedCDCTopics() {
        val topic = "topic-neo"
        val config = mapOf("streams.sink.enabled" to "false",
                "streams.sink.topic.cdc.sourceId" to topic,
                "streams.sink.topic.cdc.schema" to topic)
        streamsConfig.config.putAll(config)
        StreamsSinkConfiguration.from(streamsConfig, defalutDbName)
    }

    companion object {
        fun testDefaultConf(default: StreamsSinkConfiguration) {
            assertEquals(emptyMap(), default.topics.cypherTopics)
        }
        fun testFromConf(streamsConfig: StreamsSinkConfiguration, topic: String, topicValue: String) {
            assertEquals(1, streamsConfig.topics.cypherTopics.size)
            assertEquals(topicValue, streamsConfig.topics.cypherTopics[topic])
        }
    }

}