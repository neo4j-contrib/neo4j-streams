package streams.integrations

import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import extension.newDatabase
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.junit.After
import org.junit.AfterClass
import org.junit.Assume
import org.junit.BeforeClass
import org.junit.Test
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.test.TestGraphDatabaseFactory
import org.testcontainers.containers.KafkaContainer
import streams.kafka.KafkaConfiguration
import streams.kafka.KafkaTestUtils
import streams.utils.StreamsUtils
import kotlin.test.assertEquals

@Suppress("UNCHECKED_CAST", "DEPRECATION")
class KafkaEventRouterNoTopicAutocreationIT {

    companion object {
        /**
         * Kafka TestContainers uses Confluent OSS images.
         * We need to keep in mind which is the right Confluent Platform version for the Kafka version this project uses
         *
         * Confluent Platform | Apache Kafka
         *                    |
         * 4.0.x	          | 1.0.x
         * 4.1.x	          | 1.1.x
         * 5.0.x	          | 2.0.x
         *
         * Please see also https://docs.confluent.io/current/installation/versions-interoperability.html#cp-and-apache-kafka-compatibility
         */
        private const val confluentPlatformVersion = "4.0.2"
        @JvmStatic
        lateinit var kafka: KafkaContainer

        @BeforeClass @JvmStatic
        fun setUpContainer() {
            var exists = false
            StreamsUtils.ignoreExceptions({
                kafka = KafkaContainer(confluentPlatformVersion)
                kafka.withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
                kafka.start()
                exists = true
            }, IllegalStateException::class.java)
            Assume.assumeTrue("Kafka container has to exist", exists)
            Assume.assumeTrue("Kafka must be running", kafka.isRunning)

            val client = AdminClient.create(mapOf("bootstrap.servers" to kafka.bootstrapServers.substring("PLAINTEXT://".length)))
            val topicsToCreate = listOf("person")
            client.createTopics(topicsToCreate.map { NewTopic(it, 1, 1) })
                    .all()
                    .get()
        }

        @AfterClass @JvmStatic
        fun tearDownContainer() {
            StreamsUtils.ignoreExceptions({
                kafka.stop()
            }, UninitializedPropertyAccessException::class.java)
        }
    }

    private lateinit var db: GraphDatabaseAPI

    @After
    fun shutdown() {
        db.shutdown()
    }

    @Test
    fun `should start even with no topic created`() {
        // when
        db = TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig("kafka.bootstrap.servers", kafka.bootstrapServers)
                .setConfig("streams.source.topic.nodes.personNotDefined", "Person{*}")
                .newDatabase() as GraphDatabaseAPI

        // then
        val count = db.execute("MATCH (n) RETURN COUNT(n) AS count")
                .columnAs<Long>("count")
                .next()
        assertEquals(0L, count)
    }

    @Test
    fun `should insert data without hanging`() = runBlocking {
        // given
        val personTopic = "person"
        val customerTopic = "customer"
        val neo4jTopic = "neo4j"
        val expectedTopics = listOf(personTopic, customerTopic, neo4jTopic)
        db = TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig("kafka.bootstrap.servers", kafka.bootstrapServers)
                .setConfig("streams.source.topic.nodes.$personTopic", "Person{*}")
                .setConfig("streams.source.topic.nodes.$customerTopic", "Customer{*}")
                .newDatabase() as GraphDatabaseAPI
        // we create a new node an check that the source plugin is working
        db.execute("CREATE (p:Person{id: 1})").close()
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        val consumer = KafkaTestUtils.createConsumer(config)
        consumer.subscribe(expectedTopics)
        // the consumer consumes the message from the topic
        consumer.use {
            val records = it.poll(5000)
            assertEquals(1, records.count())
        }

        // when
        val waitFor = 10000L
        withTimeout(waitFor) { // n.b. the default value for `max.block.ms` is 60 seconds so if exceeds `waitFor` throws a CancellationException
            async { db.execute("CREATE (p:Customer{id: 2})").close() }.await()
        }

        // then
        val count = db.execute("MATCH (n) RETURN COUNT(n) AS count")
                .columnAs<Long>("count")
                .next()
        assertEquals(2L, count)
    }

}