package streams

import org.junit.Test
import org.neo4j.kernel.configuration.Config
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
        val config = Config.builder()
                .withSetting("streams.sink.polling.interval", pollingInterval)
                .withSetting(topicKey, topicValue)
                .withSetting("streams.sink.enabled", "false")
                .withSetting("streams.sink.topic.cdc.merge", cdctopic)
                .build()
        val streamsConfig = StreamsSinkConfiguration.from(config)
        testFromConf(streamsConfig, pollingInterval, topic, topicValue)
        assertFalse { streamsConfig.enabled }
        assertTrue { streamsConfig.cdcMergeTopics.size == 1 && streamsConfig.cdcMergeTopics.contains(cdctopic) }
    }

    @Test(expected = RuntimeException::class)
    fun shouldFailWithCrossDefinedTopics() {
        val pollingInterval = "10"
        val topic = "topic-neo"
        val topicKey = "streams.sink.topic.cypher.$topic"
        val topicValue = "MERGE (n:Label{ id: event.id }) "
        val config = Config.builder()
                .withSetting("streams.sink.polling.interval", pollingInterval)
                .withSetting(topicKey, topicValue)
                .withSetting("streams.sink.enabled", "false")
                .withSetting("streams.sink.topic.cdc.merge", topic)
                .build()
        StreamsSinkConfiguration.from(config)
    }

    companion object {
        fun testDefaultConf(default: StreamsSinkConfiguration) {
            assertEquals(emptyMap(), default.cypherTopics)
            assertEquals(10000, default.sinkPollingInterval)
        }
        fun testFromConf(streamsConfig: StreamsSinkConfiguration, pollingInterval: String, topic: String, topicValue: String) {
            assertEquals(pollingInterval.toLong(), streamsConfig.sinkPollingInterval)
            assertEquals(1, streamsConfig.cypherTopics.size)
            assertTrue { streamsConfig.cypherTopics.containsKey(topic) }
            assertEquals(topicValue, streamsConfig.cypherTopics[topic])
        }
    }

}