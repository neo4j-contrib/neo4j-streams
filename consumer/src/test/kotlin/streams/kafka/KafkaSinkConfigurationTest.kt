package streams.kafka

import org.junit.Test
import org.neo4j.kernel.configuration.Config
import streams.StreamsSinkConfiguration
import streams.StreamsSinkConfigurationTest
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class KafkaSinkConfigurationTest {

    @Test
    fun shoulReturnDefaultConfiguration() {
        val default = KafkaSinkConfiguration()
        StreamsSinkConfigurationTest.testDefaultConf(default.streamsSinkConfiguration)

        assertEquals("localhost:2181", default.zookeeperConnect)
        assertEquals("localhost:9092", default.bootstrapServers)
        assertEquals("neo4j", default.groupId)
        assertEquals("earliest", default.autoOffsetReset)
        assertEquals(emptyMap(), default.extraProperties)
    }

    @Test
    fun shouldReturnConfigurationFromMap() {
        val pollingInterval = "10"
        val topic = "topic-neo"
        val topicKey = "streams.sink.topic.cypher.$topic"
        val topicValue = "MERGE (n:Label{ id: event.id }) "
        val zookeeper = "zookeeper:2181"
        val bootstrap = "bootstrap:9092"
        val group = "foo"
        val autoOffsetReset = "latest"
        val batchSize = "84"
        val batchTimeout = "1984"
        val config = Config.builder()
                .withSetting("streams.sink.polling.interval", pollingInterval)
                .withSetting(topicKey, topicValue)
                .withSetting("kafka.zookeeper.connect", zookeeper)
                .withSetting("kafka.bootstrap.servers", bootstrap)
                .withSetting("kafka.auto.offset.reset", autoOffsetReset)
                .withSetting("kafka.group.id", group)
                .withSetting("streams.sink.batch.size", batchSize)
                .withSetting("streams.sink.batch.timeout", batchTimeout)
                .build()
        val kafkaSinkConfiguration = KafkaSinkConfiguration.from(config)
        StreamsSinkConfigurationTest.testFromConf(kafkaSinkConfiguration.streamsSinkConfiguration, pollingInterval, topic, topicValue, batchSize, batchTimeout)
        assertEquals(emptyMap(), kafkaSinkConfiguration.extraProperties)
        assertEquals(zookeeper, kafkaSinkConfiguration.zookeeperConnect)
        assertEquals(bootstrap, kafkaSinkConfiguration.bootstrapServers)
        assertEquals(autoOffsetReset, kafkaSinkConfiguration.autoOffsetReset)
        assertEquals(group, kafkaSinkConfiguration.groupId)

        val streamsConfig = StreamsSinkConfiguration.from(config)
        assertEquals(pollingInterval.toLong(), streamsConfig.sinkPollingInterval)
        assertTrue { streamsConfig.topics.containsKey(topic) }
        assertEquals(topicValue, streamsConfig.topics[topic])
    }

}