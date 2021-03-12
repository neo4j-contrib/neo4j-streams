package streams

import org.junit.Test
import streams.service.TopicType
import streams.service.TopicValidationException
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class StreamsSinkConfigurationTest {

    private val defaultDbName = "neo4j"

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
        val streamsSinkConf = StreamsSinkConfiguration.from(config, defaultDbName, isDefaultDb = true)
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
        val streamsSinkConf = StreamsSinkConfiguration.from(config, "foo", isDefaultDb = false)
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
        val apocTimeout = "10000"
        val apocInterval = "2000"
        val clusterOnly = "true"
        val writeableInstanceInterval = "99"
        val pollIntervall = "100"
        val config = mapOf(topicKey to topicValue,
                "streams.sink.enabled" to "false",
                "streams.sink.topic.cdc.sourceId" to cdctopic,
                "streams.sink.topic.cdc.sourceId.labelName" to customLabel,
                "streams.check.apoc.timeout" to apocTimeout,
                "streams.check.apoc.interval" to apocInterval,
                "streams.sink.topic.cdc.sourceId.idName" to customId,
                "streams.cluster.only" to clusterOnly,
                "streams.sink.poll.interval" to pollIntervall,
                "streams.check.writeable.instance.interval" to writeableInstanceInterval)
        val streamsSinkConf = StreamsSinkConfiguration.from(config, defaultDbName, isDefaultDb = true)
        testFromConf(streamsSinkConf, topic, topicValue)
        assertFalse { streamsSinkConf.enabled }
        assertTrue { streamsSinkConf.clusterOnly }
        assertEquals(setOf(cdctopic), streamsSinkConf.topics.asMap()[TopicType.CDC_SOURCE_ID])
        assertEquals(customLabel, streamsSinkConf.sourceIdStrategyConfig.labelName)
        assertEquals(customId, streamsSinkConf.sourceIdStrategyConfig.idName)
        assertEquals(pollIntervall.toLong(), streamsSinkConf.pollInterval)
        assertEquals(apocTimeout.toLong(), streamsSinkConf.checkApocTimeout)
        assertEquals(apocInterval.toLong(), streamsSinkConf.checkApocInterval)
        assertEquals(writeableInstanceInterval.toLong(), streamsSinkConf.checkWriteableInstanceInterval)
    }

    @Test
    fun shouldReturnConfigurationFromMapWithNonLowerCaseDbName() {
        val topic = "mytopic"
        val topicKey = "streams.sink.topic.cypher.$topic.to.nonLowerCaseDb"
        val topicValue = "MERGE (n:Label{ id: event.id })"
        val config = mapOf(
                topicKey to topicValue,
                "streams.sink.enabled" to "false",
                "streams.sink.enabled.to.nonLowerCaseDb" to "true")
        val streamsSinkConf = StreamsSinkConfiguration.from(config, "nonlowercasedb", isDefaultDb = false)
        assertFalse { streamsSinkConf.enabled }
        assertEquals(topicValue, streamsSinkConf.topics.cypherTopics[topic])
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
        StreamsSinkConfiguration.from(config, defaultDbName, isDefaultDb = true)
    }

    @Test(expected = TopicValidationException::class)
    fun shouldFailWithCrossDefinedCDCTopics() {
        val topic = "topic-neo"
        val config = mapOf("streams.sink.enabled" to "false",
                "streams.sink.topic.cdc.sourceId" to topic,
                "streams.sink.topic.cdc.schema" to topic)
        StreamsSinkConfiguration.from(config, defaultDbName, isDefaultDb = true)
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