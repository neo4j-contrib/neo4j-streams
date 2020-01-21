package integrations.kafka

import io.confluent.kafka.serializers.KafkaAvroSerializer
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.hamcrest.Matchers
import org.junit.After
import org.junit.AfterClass
import org.junit.Assume
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.neo4j.driver.SessionConfig
import org.neo4j.function.ThrowingSupplier
import org.neo4j.test.assertion.Assert
import streams.KafkaTestUtils
import streams.Neo4jContainerExtension
import streams.serialization.JSONUtils
import streams.utils.StreamsUtils
import java.util.UUID
import java.util.concurrent.TimeUnit

class KafkaEventSinkEnterpriseTSE {

    companion object {

        private var startedFromSuite = true
        val DB_NAME_NAMES = listOf("foo", "bar")

        @JvmStatic
        val neo4j = Neo4jContainerExtension()
//                .withLogging()

        @BeforeClass
        @JvmStatic
        fun setUpContainer() {
            if (!KafkaEventSinkSuiteIT.isRunning) {
                startedFromSuite = false
                KafkaEventSinkSuiteIT.setUpContainer()
            }
            StreamsUtils.ignoreExceptions({
                neo4j.withKafka(KafkaEventSinkSuiteIT.kafka)
                        .withNeo4jConfig("streams.source.enabled", "false") // we disable the source plugin globally
                        .withNeo4jConfig("streams.sink.enabled", "false") // we disable the sink plugin globally
                DB_NAME_NAMES.forEach { neo4j.withNeo4jConfig("streams.sink.enabled.to.$it", "true") } // we enable the sink plugin only for the instances
                neo4j.withNeo4jConfig("streams.sink.topic.cypher.enterpriseCypherTopic.to.foo", "MERGE (c:Customer_foo {id: event.id, foo: 'foo'})")
                neo4j.withNeo4jConfig("streams.sink.topic.cypher.enterpriseCypherTopic.to.bar", "MERGE (c:Customer_bar {id: event.id, bar: 'bar'})")
                neo4j.withDatabases("foo", "bar", "baz")
                neo4j.start()
                Assume.assumeTrue("Neo4j must be running", neo4j.isRunning)
            }, IllegalStateException::class.java)
        }

        @AfterClass
        @JvmStatic
        fun tearDownContainer() {
            neo4j.stop()
            if (!startedFromSuite) {
                KafkaEventSinkSuiteIT.tearDownContainer()
            }
        }
    }

    lateinit var kafkaProducer: KafkaProducer<String, ByteArray>
    lateinit var kafkaAvroProducer: KafkaProducer<GenericRecord, GenericRecord>

    @Before
    fun setUp() {
        kafkaProducer = KafkaTestUtils.createProducer(
                bootstrapServers = KafkaEventSinkSuiteIT.kafka.bootstrapServers)
        kafkaAvroProducer = KafkaTestUtils.createProducer(
                bootstrapServers = KafkaEventSinkSuiteIT.kafka.bootstrapServers,
                schemaRegistryUrl = KafkaEventSinkSuiteIT.schemaRegistry.getSchemaRegistryUrl(),
                keySerializer = KafkaAvroSerializer::class.java.name,
                valueSerializer = KafkaAvroSerializer::class.java.name)
    }

    @After
    fun tearDown() {
        kafkaProducer.close()
        kafkaAvroProducer.close()
    }

    private fun getData(dbName: String): List<Map<String, Any>> {
        return neo4j.driver!!.session(SessionConfig.forDatabase(dbName))
            .beginTransaction().use { tx ->
                tx.run("MATCH (n) RETURN n").list()
                        .map { it["n"].asNode().asMap() }
            }
    }

    @Test
    fun `every instance should consume the same topic and create the its own graph`() = runBlocking {
        // given
        val producerRecord = ProducerRecord("enterpriseCypherTopic",
                UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(mapOf("id" to 1)))

        // when
        kafkaProducer.send(producerRecord).get()
        delay(5000)

        // then
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val nodes = getData("foo")
            1 == nodes.size && mapOf("id" to 1L, "foo" to "foo") == nodes[0]
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val nodes = getData("bar")
            1 == nodes.size && mapOf("id" to 1L, "bar" to "bar") == nodes[0]
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)

        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val nodes = getData("neo4j")
            nodes.isEmpty()
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val nodes = getData("baz")
            nodes.isEmpty()
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }
}