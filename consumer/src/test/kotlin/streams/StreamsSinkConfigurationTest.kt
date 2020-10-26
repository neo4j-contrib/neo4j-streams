package streams

import org.junit.Test
import org.neo4j.kernel.configuration.Config
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
        val config = Config.builder()
                .withSetting(topicKey, topicValue)
                .withSetting("streams.sink.enabled", "false")
                .withSetting("streams.sink.topic.cdc.sourceId", cdctopic)
                .withSetting("streams.sink.topic.cdc.sourceId.labelName", customLabel)
                .withSetting("streams.sink.topic.cdc.sourceId.idName", customId)
                .withSetting("streams.check.apoc.timeout", apocTimeout)
                .withSetting("streams.check.apoc.interval", apocInterval)
                .withSetting("streams.cluster.only", clusterOnly)
                .withSetting("streams.sink.poll.interval", pollIntervall)
                .withSetting("streams.check.writeable.instance.interval", writeableInstanceInterval)
                .build()
        val streamsConfig = StreamsSinkConfiguration.from(config)
        testFromConf(streamsConfig, topic, topicValue)
        assertFalse { streamsConfig.enabled }
        assertTrue { streamsConfig.clusterOnly }
        assertEquals(setOf(cdctopic), streamsConfig.topics.asMap()[TopicType.CDC_SOURCE_ID])
        assertEquals(customLabel, streamsConfig.sourceIdStrategyConfig.labelName)
        assertEquals(writeableInstanceInterval.toLong(), streamsConfig.checkWriteableInstanceInterval)
        assertEquals(customId, streamsConfig.sourceIdStrategyConfig.idName)
        assertEquals(apocTimeout.toLong(), streamsConfig.checkApocTimeout)
        assertEquals(apocInterval.toLong(), streamsConfig.checkApocInterval)
        assertEquals(pollIntervall.toLong(), streamsConfig.pollInterval)
    }

    @Test(expected = RuntimeException::class)
    fun shouldFailWithCrossDefinedTopics() {
        val topic = "topic-neo"
        val topicKey = "streams.sink.topic.cypher.$topic"
        val topicValue = "MERGE (n:Label{ id: event.id }) "
        val config = Config.builder()
                .withSetting(topicKey, topicValue)
                .withSetting("streams.sink.topic.pattern.node.nodePatternTopic", "User{!userId,name,surname,address.city}")
                .withSetting("streams.sink.enabled", "false")
                .withSetting("streams.sink.topic.cdc.sourceId", topic)
                .build()
        StreamsSinkConfiguration.from(config)
    }

    @Test(expected = RuntimeException::class)
    fun shouldFailWithCrossDefinedCDCTopics() {
        val topic = "topic-neo"
        val config = Config.builder()
                .withSetting("streams.sink.enabled", "false")
                .withSetting("streams.sink.topic.cdc.sourceId", topic)
                .withSetting("streams.sink.topic.cdc.schema", topic)
                .build()
        StreamsSinkConfiguration.from(config)
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