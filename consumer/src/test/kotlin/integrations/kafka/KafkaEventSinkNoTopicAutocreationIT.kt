package integrations.kafka

import integrations.kafka.KafkaTestUtils.createProducer
import extension.newDatabase
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.hamcrest.Matchers
import org.junit.AfterClass
import org.junit.Assume
import org.junit.BeforeClass
import org.junit.Test
import org.neo4j.function.ThrowingSupplier
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.test.TestGraphDatabaseFactory
import org.neo4j.test.assertion.Assert
import org.testcontainers.containers.KafkaContainer
import streams.serialization.JSONUtils
import streams.utils.StreamsUtils
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.test.assertTrue

class KafkaEventSinkNoTopicAutoCreationIT {
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

        @BeforeClass
        @JvmStatic
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
        }

        @AfterClass
        @JvmStatic
        fun tearDownContainer() {
            StreamsUtils.ignoreExceptions({
                kafka.stop()
            }, UninitializedPropertyAccessException::class.java)
        }
    }

    @Test
    fun `should consume only the registered topic`() {
        // given
        val topic = "shouldWriteCypherQuery"
        val client = AdminClient.create(mapOf("bootstrap.servers" to kafka.bootstrapServers.substring("PLAINTEXT://".length)))
        val expectedTopics = listOf(topic)
        client.createTopics(expectedTopics.map { NewTopic(it, 1, 1) })
                .all()
                .get()
        val topicList = client.listTopics().names().get()
        val notRegisteredTopic = "notRegistered"
        assertTrue { topicList.containsAll(expectedTopics.toSet()) && !topicList.contains(notRegisteredTopic) }
        val db = TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig("kafka.bootstrap.servers", kafka.bootstrapServers)
                .setConfig("streams.sink.enabled", "true")
                .setConfig("streams.sink.topic.cypher.$notRegisteredTopic", "MERGE (p:NotRegisteredTopic{name: event.name})")
                .setConfig("streams.sink.topic.cypher.$topic", "MERGE (p:Person{name: event.name})")
                .newDatabase() as GraphDatabaseAPI
        val kafkaProducer: KafkaProducer<String, ByteArray> = createProducer(kafka = kafka)

        // when
        val data = mapOf<String, Any>("name" to "Andrea")
        val producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(data))
        kafkaProducer.send(producerRecord).get()

        // then
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val count = db.execute("MATCH (n:Person) RETURN COUNT(n) AS count")
                    .columnAs<Long>("count")
                    .next()
            val topics = client.listTopics().names().get()
            count == 1L && !topics.contains(notRegisteredTopic)
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
        db.shutdown()
    }
}