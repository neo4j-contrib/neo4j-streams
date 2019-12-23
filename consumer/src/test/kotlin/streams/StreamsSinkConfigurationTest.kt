package streams

import org.junit.Test
import org.neo4j.configuration.Config
import streams.service.TopicType
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class StreamsSinkConfigurationTest {

    @Test
    fun shouldReturnDefaultConfiguration() {
        val default = StreamsSinkConfiguration()
        testDefaultConf(default)
    }

    @Test
    fun shouldReturnConfigurationFromMap() {
        val pollingInterval = "10"
        val topic = "topic-neo"
        val cdctopic = "cdctopic"
        val topicKey = "streams.sink.topic.cypher.$topic"
        val topicValue = "MERGE (n:Label{ id: event.id }) "
        val customLabel = "CustomLabel"
        val customId = "customId"
        val config = mapOf(
                "streams.sink.polling.interval" to pollingInterval,
                topicKey to topicValue,
                "streams.sink.enabled" to "false",
                "streams.sink.topic.cdc.sourceId" to cdctopic,
                "streams.sink.topic.cdc.sourceId.labelName" to customLabel,
                "streams.sink.topic.cdc.sourceId.idName" to customId)
        val streamsConfig = StreamsSinkConfiguration.from(config)
        testFromConf(streamsConfig, pollingInterval, topic, topicValue)
        assertFalse { streamsConfig.enabled }
        assertEquals(setOf(cdctopic), streamsConfig.topics.asMap()[TopicType.CDC_SOURCE_ID])
        assertEquals(customLabel, streamsConfig.sourceIdStrategyConfig.labelName)
        assertEquals(customId, streamsConfig.sourceIdStrategyConfig.idName)
    }

    @Test(expected = RuntimeException::class)
    fun shouldFailWithCrossDefinedTopics() {
        val pollingInterval = "10"
        val topic = "topic-neo"
        val topicKey = "streams.sink.topic.cypher.$topic"
        val topicValue = "MERGE (n:Label{ id: event.id }) "
        val config = mapOf("streams.sink.polling.interval" to pollingInterval,
                topicKey to topicValue,
                "streams.sink.topic.pattern.node.nodePatternTopic" to "User{!userId,name,surname,address.city}",
                "streams.sink.enabled" to "false",
                "streams.sink.topic.cdc.sourceId" to topic)
        StreamsSinkConfiguration.from(config)
    }

    @Test(expected = RuntimeException::class)
    fun shouldFailWithCrossDefinedCDCTopics() {
        val pollingInterval = "10"
        val topic = "topic-neo"
        val config = mapOf("streams.sink.polling.interval" to pollingInterval,
                "streams.sink.enabled" to "false",
                "streams.sink.topic.cdc.sourceId" to topic,
                "streams.sink.topic.cdc.schema" to topic)
        StreamsSinkConfiguration.from(config)
    }

    companion object {
        fun testDefaultConf(default: StreamsSinkConfiguration) {
            assertEquals(emptyMap(), default.topics.cypherTopics)
            assertEquals(10000, default.sinkPollingInterval)
        }
        fun testFromConf(streamsConfig: StreamsSinkConfiguration, pollingInterval: String, topic: String, topicValue: String) {
            assertEquals(pollingInterval.toLong(), streamsConfig.sinkPollingInterval)
            assertEquals(1, streamsConfig.topics.cypherTopics.size)
            assertEquals(topicValue, streamsConfig.topics.cypherTopics[topic])
        }
    }

}