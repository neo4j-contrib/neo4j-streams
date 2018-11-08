package streams

import org.junit.Test
import org.neo4j.kernel.configuration.Config
import kotlin.test.assertEquals
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
        val topicKey = "streams.sink.topic.cypher.$topic"
        val topicValue = "MERGE (n:Label{ id: event.id }) "
        val config = Config.builder()
                .withSetting("streams.sink.polling.interval", pollingInterval)
                .withSetting(topicKey, topicValue)
                .build()
        val streamsConfig = StreamsSinkConfiguration.from(config)
        testFromConf(streamsConfig, pollingInterval, topic, topicValue)
    }

    companion object {
        fun testDefaultConf(default: StreamsSinkConfiguration) {
            assertEquals(emptyMap(), default.topics)
            assertEquals(Long.MAX_VALUE, default.sinkPollingInterval)
        }
        fun testFromConf(streamsConfig: StreamsSinkConfiguration, pollingInterval: String, topic: String, topicValue: String) {
            assertEquals(pollingInterval.toLong(), streamsConfig.sinkPollingInterval)
            assertEquals(1, streamsConfig.topics.size)
            assertTrue { streamsConfig.topics.containsKey(topic) }
            assertEquals(topicValue, streamsConfig.topics[topic])
        }
    }

}