package streams.kafka

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.junit.Before
import org.junit.Ignore
import org.junit.Test
import org.mockito.Mockito
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.ResourceIterator
import org.neo4j.graphdb.Result
import org.neo4j.graphdb.Transaction
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.NullLog
import streams.StreamsSinkConfiguration
import streams.StreamsSinkConfigurationTest
import streams.config.StreamsConfig
import streams.service.TopicValidationException
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class KafkaSinkConfigurationTest {

    lateinit var streamsConfig: StreamsConfig
    val defaultDbName = "neo4j"

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
        Mockito.`when`(resource.next()).thenReturn(defaultDbName)
        streamsConfig = StreamsConfig(NullLog.getInstance(), dbms)
    }

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
        val config = mapOf(topicKey to topicValue,
                "kafka.zookeeper.connect" to zookeeper,
                "kafka.bootstrap.servers" to bootstrap,
                "kafka.auto.offset.reset" to autoOffsetReset,
                "kafka.enable.auto.commit" to autoCommit,
                "kafka.group.id" to group,
                "kafka.streams.async.commit" to "true",
                "kafka.key.deserializer" to ByteArrayDeserializer::class.java.name,
                "kafka.value.deserializer" to KafkaAvroDeserializer::class.java.name)
        val expectedMap = mapOf("zookeeper.connect" to zookeeper, "bootstrap.servers" to bootstrap,
                "auto.offset.reset" to autoOffsetReset, "enable.auto.commit" to autoCommit, "group.id" to group,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java.toString(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java.toString(),
                "streams.async.commit" to "true",
                "key.deserializer" to ByteArrayDeserializer::class.java.name,
                "value.deserializer" to KafkaAvroDeserializer::class.java.name)

        streamsConfig.config.putAll(config)
        val kafkaSinkConfiguration = KafkaSinkConfiguration.create(streamsConfig, defaultDbName)
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

        val streamsConfig = StreamsSinkConfiguration.from(streamsConfig, defaultDbName)
        assertTrue { streamsConfig.topics.cypherTopics.containsKey(topic) }
        assertEquals(topicValue, streamsConfig.topics.cypherTopics[topic])
    }

    @Test
    fun `should return configuration from map for non default DB`() {
        val dbName = "foo"
        val topic = "topic-neo"
        val topicKey = "streams.sink.topic.cypher.$topic"
        val topicValue = "MERGE (n:Label{ id: event.id })"
        val topicKeyFoo = "streams.sink.topic.cypher.$topic.to.foo"
        val topicValueFoo = "MERGE (n:Foo{ id: event.id })"
        val zookeeper = "zookeeper:2181"
        val bootstrap = "bootstrap:9092"
        val group = "mygroup"
        val autoOffsetReset = "latest"
        val autoCommit = "false"
        val asyncCommit = "true"
        val config = mapOf(topicKey to topicValue,
                topicKeyFoo to topicValueFoo,
                "kafka.zookeeper.connect" to zookeeper,
                "kafka.bootstrap.servers" to bootstrap,
                "kafka.auto.offset.reset" to autoOffsetReset,
                "kafka.enable.auto.commit" to autoCommit,
                "kafka.group.id" to group,
                "kafka.streams.async.commit" to asyncCommit,
                "kafka.key.deserializer" to ByteArrayDeserializer::class.java.name,
                "kafka.value.deserializer" to KafkaAvroDeserializer::class.java.name)
        val expectedMap = mapOf("zookeeper.connect" to zookeeper, "bootstrap.servers" to bootstrap,
                "auto.offset.reset" to autoOffsetReset, "enable.auto.commit" to autoCommit, "group.id" to "$group-$dbName",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java.toString(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java.toString(),
                "key.deserializer" to ByteArrayDeserializer::class.java.name,
                "streams.async.commit" to asyncCommit,
                "value.deserializer" to KafkaAvroDeserializer::class.java.name)

        streamsConfig.config.putAll(config)
        val kafkaSinkConfiguration = KafkaSinkConfiguration.create(streamsConfig, dbName)
        StreamsSinkConfigurationTest.testFromConf(kafkaSinkConfiguration.streamsSinkConfiguration, topic, topicValueFoo)
        assertEquals(emptyMap(), kafkaSinkConfiguration.extraProperties)
        assertEquals(zookeeper, kafkaSinkConfiguration.zookeeperConnect)
        assertEquals(bootstrap, kafkaSinkConfiguration.bootstrapServers)
        assertEquals(autoOffsetReset, kafkaSinkConfiguration.autoOffsetReset)
        assertTrue { kafkaSinkConfiguration.streamsAsyncCommit }
        assertEquals("$group-$dbName", kafkaSinkConfiguration.groupId)
        val resultMap = kafkaSinkConfiguration
                .asProperties()
                .map { it.key.toString() to it.value.toString() }
                .toMap()
        assertEquals(expectedMap, resultMap)

        val streamsConfig = StreamsSinkConfiguration.from(streamsConfig, dbName)
        assertEquals(1, streamsConfig.topics.cypherTopics.size)
        assertTrue { streamsConfig.topics.cypherTopics.containsKey(topic) }
        assertEquals(topicValueFoo, streamsConfig.topics.cypherTopics[topic])
    }

    @Test(expected = TopicValidationException::class)
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
            val config = mapOf(topicKey to topicValue,
                    "$topicKey.to.foo" to "$topicValue SET n += event.properties",
                    "kafka.zookeeper.connect" to zookeeper,
                    "kafka.bootstrap.servers" to bootstrap,
                    "kafka.auto.offset.reset" to autoOffsetReset,
                    "kafka.enable.auto.commit" to autoCommit,
                    "kafka.group.id" to group,
                    "kafka.key.deserializer" to ByteArrayDeserializer::class.java.name,
                    "kafka.value.deserializer" to KafkaAvroDeserializer::class.java.name)
            streamsConfig.config.putAll(config)
            KafkaSinkConfiguration.from(streamsConfig, defaultDbName)
        } catch (e: TopicValidationException) {
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
            val config = mapOf(topicKey to topicValue,
                    "kafka.zookeeper.connect" to zookeeper,
                    "kafka.bootstrap.servers" to bootstrap,
                    "kafka.auto.offset.reset" to autoOffsetReset,
                    "kafka.enable.auto.commit" to autoCommit,
                    "kafka.group.id" to group,
                    "kafka.key.deserializer" to ByteArrayDeserializer::class.java.name,
                    "kafka.value.deserializer" to KafkaAvroDeserializer::class.java.name)
            streamsConfig.config.putAll(config)
            KafkaSinkConfiguration.from(streamsConfig, defaultDbName)
        } catch (e: RuntimeException) {
            assertEquals("The `kafka.bootstrap.servers` property is empty", e.message)
            throw e
        }
    }

}