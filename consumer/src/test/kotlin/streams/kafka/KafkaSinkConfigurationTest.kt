package streams.kafka

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.Ignore
import org.junit.Test
import org.neo4j.kernel.configuration.Config
import streams.StreamsSinkConfiguration
import streams.StreamsSinkConfigurationTest
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class KafkaSinkConfigurationTest {

    @Test
    fun `should return default configuration`() {
        val default = KafkaSinkConfiguration()
        StreamsSinkConfigurationTest.testDefaultConf(default.streamsSinkConfiguration)

        assertEquals("localhost:2181", default.zookeeperConnect)
        assertEquals("localhost:9092", default.bootstrapServers)
        assertEquals("neo4j", default.groupId)
        assertEquals("earliest", default.autoOffsetReset)
        assertEquals(ByteArrayDeserializer::class.java.name, default.keyDeserializer)
        assertEquals(ByteArrayDeserializer::class.java.name, default.valueDeserializer)
        assertEquals(true, default.enableAutoCommit)
        assertEquals(false, default.streamsAsyncCommit)
        assertEquals(emptyMap(), default.extraProperties)
    }

    @Test
    fun `should return configuration from map`() {
        val topic = "topic-neo"
        val topicKey = "streams.sink.topic.cypher.$topic"
        val topicValue = "MERGE (n:Label{ id: event.id }) "
        val zookeeper = "zookeeper:2181"
        val bootstrap = "bootstrap:9092"
        val group = "foo"
        val autoOffsetReset = "latest"
        val autoCommit = "false"
        val config = Config.builder()
                .withSetting(topicKey, topicValue)
                .withSetting("kafka.zookeeper.connect", zookeeper)
                .withSetting("kafka.bootstrap.servers", bootstrap)
                .withSetting("kafka.auto.offset.reset", autoOffsetReset)
                .withSetting("kafka.enable.auto.commit", autoCommit)
                .withSetting("kafka.group.id", group)
                .withSetting("kafka.streams.async.commit", "true")
                .withSetting("kafka.key.deserializer", ByteArrayDeserializer::class.java.name)
                .withSetting("kafka.value.deserializer", KafkaAvroDeserializer::class.java.name)
                .build()
        val expectedMap = mapOf("zookeeper.connect" to zookeeper, "bootstrap.servers" to bootstrap,
                "auto.offset.reset" to autoOffsetReset, "enable.auto.commit" to autoCommit, "group.id" to group,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java.toString(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java.toString(),
                "streams.async.commit" to "true",
                "key.deserializer" to ByteArrayDeserializer::class.java.name,
                "value.deserializer" to KafkaAvroDeserializer::class.java.name)

        val kafkaSinkConfiguration = KafkaSinkConfiguration.create(config.raw)
        StreamsSinkConfigurationTest.testFromConf(kafkaSinkConfiguration.streamsSinkConfiguration, topic, topicValue)
        assertEquals(emptyMap(), kafkaSinkConfiguration.extraProperties)
        assertEquals(zookeeper, kafkaSinkConfiguration.zookeeperConnect)
        assertEquals(bootstrap, kafkaSinkConfiguration.bootstrapServers)
        assertEquals(autoOffsetReset, kafkaSinkConfiguration.autoOffsetReset)
        assertEquals(group, kafkaSinkConfiguration.groupId)
        val resultMap = kafkaSinkConfiguration
                .asProperties()
                .map { it.key.toString() to it.value.toString() }
                .toMap()
        assertEquals(expectedMap, resultMap)

        val streamsConfig = StreamsSinkConfiguration.from(config)
        assertTrue { streamsConfig.topics.cypherTopics.containsKey(topic) }
        assertEquals(topicValue, streamsConfig.topics.cypherTopics[topic])
    }

    @Test(expected = RuntimeException::class)
    @Ignore("Disabled, use Kafka to deal with availability of the configured services")
    fun `should not validate the configuration because of unreachable kafka bootstrap server`() {
        val zookeeper = "zookeeper:2181"
        val bootstrap = "bootstrap:9092"
        try {
            val topic = "topic-neo"
            val topicKey = "streams.sink.topic.cypher.$topic"
            val topicValue = "MERGE (n:Label{ id: event.id }) "
            val group = "foo"
            val autoOffsetReset = "latest"
            val autoCommit = "false"
            val config = Config.builder()
                    .withSetting(topicKey, topicValue)
                    .withSetting("kafka.zookeeper.connect", zookeeper)
                    .withSetting("kafka.bootstrap.servers", bootstrap)
                    .withSetting("kafka.auto.offset.reset", autoOffsetReset)
                    .withSetting("kafka.enable.auto.commit", autoCommit)
                    .withSetting("kafka.group.id", group)
                    .withSetting("kafka.key.deserializer", ByteArrayDeserializer::class.java.name)
                    .withSetting("kafka.value.deserializer", KafkaAvroDeserializer::class.java.name)
                    .build()
            KafkaSinkConfiguration.from(config)
        } catch (e: RuntimeException) {
            assertEquals("The servers defined into the property `kafka.bootstrap.servers` are not reachable: [$bootstrap]", e.message)
            throw e
        }
    }



    @Test(expected = RuntimeException::class)
    fun `should not validate the configuration because of empty kafka bootstrap server`() {
        val zookeeper = "zookeeper:2181"
        val bootstrap = ""
        try {
            val topic = "topic-neo"
            val topicKey = "streams.sink.topic.cypher.$topic"
            val topicValue = "MERGE (n:Label{ id: event.id }) "
            val group = "foo"
            val autoOffsetReset = "latest"
            val autoCommit = "false"
            val config = Config.builder()
                    .withSetting(topicKey, topicValue)
                    .withSetting("kafka.zookeeper.connect", zookeeper)
                    .withSetting("kafka.bootstrap.servers", bootstrap)
                    .withSetting("kafka.auto.offset.reset", autoOffsetReset)
                    .withSetting("kafka.enable.auto.commit", autoCommit)
                    .withSetting("kafka.group.id", group)
                    .withSetting("kafka.key.deserializer", ByteArrayDeserializer::class.java.name)
                    .withSetting("kafka.value.deserializer", KafkaAvroDeserializer::class.java.name)
                    .build()
            KafkaSinkConfiguration.from(config)
        } catch (e: RuntimeException) {
            assertEquals("The `kafka.bootstrap.servers` property is empty", e.message)
            throw e
        }
    }

}