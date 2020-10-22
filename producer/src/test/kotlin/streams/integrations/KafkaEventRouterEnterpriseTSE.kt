package streams.integrations

import kotlinx.coroutines.delay

import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.junit.AfterClass
import org.junit.Assume
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.neo4j.driver.SessionConfig
import streams.KafkaTestUtils
import streams.Neo4jContainerExtension
import streams.events.EntityType
import streams.events.NodeChange
import streams.events.OperationType
import streams.events.RelationshipPayload
import streams.utils.JSONUtils
import streams.utils.StreamsUtils
import kotlin.test.assertEquals

class KafkaEventRouterEnterpriseTSE {

    companion object {

        private var startedFromSuite = true
        val DB_NAME_NAMES = listOf("foo", "bar", "deletedb")

        @JvmStatic
        val neo4j = Neo4jContainerExtension()//.withLogging()

        @BeforeClass
        @JvmStatic
        fun setUpContainer() {
            // Assume.assumeFalse(MavenUtils.isTravis())
            if (!KafkaEventRouterSuiteIT.isRunning) {
                startedFromSuite = false
                KafkaEventRouterSuiteIT.setUpContainer()
            }
            StreamsUtils.ignoreExceptions({
                DB_NAME_NAMES.forEach { neo4j.withNeo4jConfig("streams.source.enabled.from.$it", "true") } // we enable the source plugin only for the instances
                neo4j.withKafka(KafkaEventRouterSuiteIT.kafka)
                        .withNeo4jConfig("streams.source.enabled", "false") // we disable the source plugin
                // for the bar instance we create custom routing params
                neo4j.withNeo4jConfig("streams.source.topic.relationships.knows.from.bar", "KNOWS{since}")
                        .withNeo4jConfig("streams.source.topic.nodes.person.from.bar", "Person{name,surname}")
                        .withNeo4jConfig("streams.source.topic.nodes.deletedb.from.deletedb", "Person{name,surname}")
                        .withNeo4jConfig("streams.source.topic.relationships.deletedb.from.deletedb", "KNOWS{since}")
                neo4j.withDatabases("foo", "bar", "baz", "deletedb")
                neo4j.start()
                Assume.assumeTrue("Neo4j must be running", neo4j.isRunning)
            }, IllegalStateException::class.java)
        }

        @AfterClass
        @JvmStatic
        fun tearDownContainer() {
            neo4j.stop()
            if (!startedFromSuite) {
                KafkaEventRouterSuiteIT.tearDownContainer()
            }
        }
    }

    @Before
    fun before() {
        DB_NAME_NAMES.forEach(this::cleanAll)
    }

    private fun createNodeAndConsumeKafkaRecords(dbName: String): ConsumerRecords<String, ByteArray> {
        // given
        neo4j.driver!!.session(SessionConfig.forDatabase(dbName)).beginTransaction().use {
            it.run("CREATE (:Person:$dbName {name:'John Doe', age:42})")
            it.commit()
        }

        // when
        val kafkaConsumerNeo = KafkaTestUtils.createConsumer<String, ByteArray>(bootstrapServers = KafkaEventRouterSuiteIT.kafka.bootstrapServers)
        return kafkaConsumerNeo.use {
            it.subscribe(setOf(dbName))
            val records = it.poll(5000)
            records
        }
    }

    @Test
    fun `should publish delete message without break`() = runBlocking {
        // given
        val dbName = "deletedb"
        createPath(dbName)
        cleanAll(dbName)
        delay(5000)

        // when
        val kafkaConsumerFoo = KafkaTestUtils
                .createConsumer<String, ByteArray>(bootstrapServers = KafkaEventRouterSuiteIT.kafka.bootstrapServers)

        // then
        kafkaConsumerFoo.use {
            it.subscribe(setOf(dbName))
            val records = it.poll(5000)
            assertEquals(6, records.count()) // foo instance should publish all the events into the foo topic
            val deletedRecords = records
                    .map { JSONUtils.asStreamsTransactionEvent(it.value()) }
                    .filter { it.meta.operation == OperationType.deleted }
            assertEquals(3, deletedRecords.count())
        }
    }

    @Test
    fun `should stream the data from a specific instance with custom routing params`() = runBlocking {
        // given
        createPath("foo")
        createPath("bar")
        delay(5000)

        // when
        var kafkaConsumerFoo = KafkaTestUtils
                .createConsumer<String, ByteArray>(
                        bootstrapServers = KafkaEventRouterSuiteIT.kafka.bootstrapServers)
        val kafkaConsumerBarKnows = KafkaTestUtils
                .createConsumer<String, ByteArray>(
                        bootstrapServers = KafkaEventRouterSuiteIT.kafka.bootstrapServers)
        val kafkaConsumerBarPerson = KafkaTestUtils
                .createConsumer<String, ByteArray>(
                        bootstrapServers = KafkaEventRouterSuiteIT.kafka.bootstrapServers)

        // then
        kafkaConsumerFoo.use {
            it.subscribe(setOf("foo"))
            val records = it.poll(5000)
            assertEquals(3, records.count()) // foo instance should publish all the events into the foo topic
        }
        // bar instance should publish all the events into two topics
        kafkaConsumerBarKnows.use {
            it.subscribe(setOf("knows")) // for the relationships
            val records = it.poll(5000)
            assertEquals(1, records.count())
            assertEquals(true, records.all {
                JSONUtils.asStreamsTransactionEvent(it.value()).let {
                    var payload = it.payload as RelationshipPayload
                    val properties = payload.after!!.properties!!
                    payload.type == EntityType.relationship && payload.label == "KNOWS"
                            && properties == mapOf("since" to 2014)
                            && it.meta.operation == OperationType.created
                }
            })
        }
        kafkaConsumerBarPerson.use {
            it.subscribe(setOf("person"))
            val records = it.poll(5000)
            assertEquals(2, records.count())
            assertEquals(true, records.all {
                JSONUtils.asStreamsTransactionEvent(it.value()).let {
                    val after = it.payload.after as NodeChange
                    val labels = after.labels
                    val propertiesAfter = after.properties
                    labels == listOf("Person", "bar") && propertiesAfter!!.keys == setOf("name", "surname")
                            && it.meta.operation == OperationType.created
                }
            })
        }

        // the other dbs should not be affected
        assertEquals(0, createNodeAndConsumeKafkaRecords("neo4j").count())
        assertEquals(0, createNodeAndConsumeKafkaRecords("baz").count())
    }

    private fun createPath(dbName: String) {
        neo4j.driver!!.session(SessionConfig.forDatabase(dbName))
                .run("""CREATE (start:Person:$dbName {name:'Andrea', surname: 'Santurbano', andreaHiddenProp: true})
                |CREATE (end:Person:$dbName {name:'Michael', surname: 'Hunger', michaelHiddenProp: true})
                |CREATE (start)-[:KNOWS{since: 2014, hiddenProp: true}]->(end)
                |""".trimMargin())
                .list()
    }

    private fun cleanAll(dbName: String) {
        neo4j.driver!!.session(SessionConfig.forDatabase(dbName))
                .run("MATCH (n) DETACH DELETE n").list()
    }

}