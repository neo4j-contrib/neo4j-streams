package streams.integrations

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.config.TopicConfig
import org.hamcrest.Matchers
import org.junit.*
import org.neo4j.driver.SessionConfig
import streams.Assert
import streams.KafkaTestUtils
import streams.Neo4jContainerExtension
import streams.events.EntityType
import streams.events.NodeChange
import streams.events.OperationType
import streams.events.RelationshipPayload
import streams.utils.JSONUtils
import streams.utils.StreamsUtils
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import org.neo4j.function.ThrowingSupplier
import streams.integrations.CompactionStrategyTestCommon.assertTopicFilled

class KafkaEventRouterEnterpriseTSE {

    companion object {

        const val DB_TEST_REL_WITH_COMPACT = "alpha"
        const val DB_TEST_NODE_WITH_COMPACT = "beta"
        const val DB_TOMBSTONE_WITH_COMPACT = "gamma"

        const val TOPIC_PERSON_AND_BUYS_IN_DB_TEST_REL = "one"
        const val TOPIC_PRODUCT_IN_DB_TEST_REL = "two"
        const val TOPIC_IN_DB_TEST_NODE = "three"
        const val TOPIC_WITH_TOMBSTONE = "four"

        lateinit var kafkaConsumer: KafkaConsumer<String, ByteArray>
        private var startedFromSuite = true
        val DB_NAME_NAMES = listOf("foo", "bar", "deletedb", DB_TEST_REL_WITH_COMPACT, DB_TEST_NODE_WITH_COMPACT, DB_TOMBSTONE_WITH_COMPACT)

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
                        .withNeo4jConfig("streams.source.schema.polling.interval", "0")
                        .withNeo4jConfig("topic.discovery.polling.interval", "0")
                        .withNeo4jConfig("kafka.streams.log.compaction.strategy", TopicConfig.CLEANUP_POLICY_COMPACT)
                        .withNeo4jConfig("streams.source.topic.nodes.$TOPIC_PERSON_AND_BUYS_IN_DB_TEST_REL.from.$DB_TEST_REL_WITH_COMPACT", "Person{*}")
                        .withNeo4jConfig("streams.source.topic.relationships.$TOPIC_PERSON_AND_BUYS_IN_DB_TEST_REL.from.$DB_TEST_REL_WITH_COMPACT", "BUYS{*}")
                        .withNeo4jConfig("streams.source.topic.nodes.$TOPIC_PRODUCT_IN_DB_TEST_REL.from.$DB_TEST_REL_WITH_COMPACT", "Product{*}")
                        .withNeo4jConfig("streams.source.topic.nodes.$TOPIC_IN_DB_TEST_NODE.from.$DB_TEST_NODE_WITH_COMPACT", "Person{*}")
                        .withNeo4jConfig("streams.source.topic.nodes.$TOPIC_WITH_TOMBSTONE.from.$DB_TOMBSTONE_WITH_COMPACT", "Person{*}")
                        .withNeo4jConfig("streams.source.topic.relationships.$TOPIC_WITH_TOMBSTONE.from.$DB_TOMBSTONE_WITH_COMPACT", "KNOWS{*}")
                neo4j.withDatabases("foo", "bar", "baz", "deletedb", DB_TEST_REL_WITH_COMPACT, DB_TEST_NODE_WITH_COMPACT, DB_TOMBSTONE_WITH_COMPACT)
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
        kafkaConsumer = KafkaTestUtils.createConsumer(bootstrapServers = KafkaEventRouterSuiteIT.kafka.bootstrapServers)
    }

    @After
    fun after() {
        kafkaConsumer.close()
    }

    private fun runQueryAndAssertIsFilled(query: String, db: String) {
        runQueryInDb(query, db)
        assertTopicFilled(kafkaConsumer)
    }

    private fun runQueryInDb(query: String, db: String) =
        neo4j.driver!!.session(SessionConfig.forDatabase(db)).beginTransaction().use {
            it.run(query)
            it.commit()
        }


    private fun createManyPersons(db: String) {
        runQueryInDb("UNWIND range(1, 999) AS id CREATE (:Person {name:id, surname: id})", db)
    }

    private fun createConstraintAndAssert(constraints: List<String>, db: String, size: Int = 1) {
        constraints.forEach { runQueryInDb(it, db) }

        Assert.assertEventually(ThrowingSupplier {
            val expectedSize: Int
            neo4j.driver!!.session(SessionConfig.forDatabase(db)).beginTransaction().use {
                expectedSize = it.run("call db.constraints()").list().size
                it.commit()
            }
            expectedSize == size
        }, Matchers.equalTo(true), 60, TimeUnit.SECONDS)
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
            val deletedRecords = records.filter { it.value() == null }
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
        val kafkaConsumerFoo = KafkaTestUtils
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

    @Test
    fun `node with node key constraint and topic compact`() {
        val queries = listOf("CREATE CONSTRAINT person ON (p:Person) ASSERT (p.name, p.surname) IS NODE KEY")
        createConstraintAndAssert(queries, DB_TEST_NODE_WITH_COMPACT)

        kafkaConsumer.subscribe(listOf(TOPIC_IN_DB_TEST_NODE))

        // we create a node with constraint and check that key is equal to constraint
        runQueryInDb("CREATE (:Person {name:'Pippo', surname: 'Topolino'})", DB_TEST_NODE_WITH_COMPACT)

        var keyNode = mapOf("ids" to mapOf("surname" to "Topolino", "name" to "Pippo"), "labels" to listOf("Person"))
        assertTopicFilled(kafkaConsumer) { it.count() == 1 &&
                JSONUtils.readValue<Map<*, *>>(it.first().key()) == keyNode
        }

        keyNode = mapOf("ids" to mapOf("surname" to "Topolino", "name" to "Pluto"), "labels" to listOf("Person"))
        // we update the node
        runQueryInDb("MATCH (p:Person {name:'Pippo'}) SET p.name='Pluto'", DB_TEST_NODE_WITH_COMPACT)
        assertTopicFilled(kafkaConsumer) { it.count() == 1 &&
                JSONUtils.readValue<Map<*, *>>(it.first().key()) == keyNode
        }

        // we delete the node
        runQueryInDb("MATCH (p:Person {name:'Pluto'}) DETACH DELETE p", DB_TEST_NODE_WITH_COMPACT)
        assertTopicFilled(kafkaConsumer) { it.count() == 1 &&
                JSONUtils.readValue<Map<*, *>>(it.first().key()) == keyNode
        }
    }

    @Test
    fun `delete single tombstone relation with strategy compact and constraints`() {
        // we create a topic with strategy compact
        val keyRel = "KNOWS"
        val queries = listOf("CREATE CONSTRAINT person ON (p:Person) ASSERT (p.name, p.surname) IS NODE KEY")
        createConstraintAndAssert(queries, DB_TOMBSTONE_WITH_COMPACT)

        kafkaConsumer.subscribe(listOf(TOPIC_WITH_TOMBSTONE))

        runQueryAndAssertIsFilled("CREATE (:Person {name:'Pippo', surname: 'Pippo_2'})", DB_TOMBSTONE_WITH_COMPACT)
        runQueryAndAssertIsFilled("CREATE (:Person {name:'Pluto', surname: 'Pluto_2'})", DB_TOMBSTONE_WITH_COMPACT)
        runQueryAndAssertIsFilled("""
            |MATCH (pippo:Person {name:'Pippo'})
            |MATCH (pluto:Person {name:'Pluto'})
            |MERGE (pippo)-[:$keyRel]->(pluto)
        """.trimMargin(), DB_TOMBSTONE_WITH_COMPACT)
        // we delete a rel, so will be created a tombstone record
        runQueryAndAssertIsFilled("MATCH (:Person {name:'Pippo'})-[rel:$keyRel]->(:Person {name:'Pluto'}) DELETE rel", DB_TOMBSTONE_WITH_COMPACT)
        runQueryAndAssertIsFilled("CREATE (:Person {name:'Sherlock', surname: 'Holmes'})", DB_TOMBSTONE_WITH_COMPACT)
        runQueryAndAssertIsFilled("MATCH (p:Person {name:'Sherlock'}) SET p.address = '221B Baker Street'", DB_TOMBSTONE_WITH_COMPACT)
        // we delete a node, so will be created a tombstone record
        runQueryAndAssertIsFilled("MATCH (p:Person {name:'Sherlock'}) DETACH DELETE p", DB_TOMBSTONE_WITH_COMPACT)
        runQueryAndAssertIsFilled("CREATE (:Person {name:'Watson', surname: 'John'})", DB_TOMBSTONE_WITH_COMPACT)

        val mapStartExpected = mapOf("ids" to mapOf("name" to "Pippo", "surname" to "Pippo_2"), "labels" to listOf("Person"))
        val mapEndExpected = mapOf("ids" to mapOf("name" to "Pluto", "surname" to "Pluto_2"), "labels" to listOf("Person"))
        val nodeRecordExpected = mapOf("ids" to mapOf("surname" to "Holmes", "name" to "Sherlock"), "labels" to listOf("Person"))

        // to activate the log compaction process we create dummy messages and waiting for messages population
        createManyPersons(DB_TOMBSTONE_WITH_COMPACT)
        assertTopicFilled(kafkaConsumer, true, 120) {
            val nullRecords = it.filter { it.value() == null }
            val relRecord: Map<String, Any>? = nullRecords.firstOrNull()?.let { JSONUtils.readValue(it.key()) }
            val nodeRecordActual: Map<String, Any>? = nullRecords.lastOrNull()?.let { JSONUtils.readValue(it.key()) }
            it.count() == 500
                    && nullRecords.count() == 2
                    && relRecord == mapOf("start" to mapStartExpected, "end" to mapEndExpected, "label" to keyRel)
                    && nodeRecordExpected == nodeRecordActual
        }
    }

    @Test
    fun `relationship with node key constraints and strategy compact`() {
        val relType = "BUYS"
        val queries = listOf("CREATE CONSTRAINT product ON (p:Product) ASSERT (p.code, p.price) IS NODE KEY",
                "CREATE CONSTRAINT other ON (p:Other) ASSERT (p.address, p.city) IS NODE KEY")
        createConstraintAndAssert(queries, DB_TEST_REL_WITH_COMPACT, 2)

        kafkaConsumer.subscribe(listOf(TOPIC_PERSON_AND_BUYS_IN_DB_TEST_REL, TOPIC_PRODUCT_IN_DB_TEST_REL))

        runQueryInDb("CREATE (:Person:Other {name: 'Sherlock', surname: 'Holmes', address: 'Baker Street', city: 'London'})", DB_TEST_REL_WITH_COMPACT)
        val keyStart = mapOf("ids" to mapOf("address" to "Baker Street", "city" to "London"), "labels" to listOf("Other", "Person"))
        assertTopicFilled(kafkaConsumer) { it.count() == 1
                && JSONUtils.readValue<Map<*, *>>(it.first().key()) == keyStart
        }

        runQueryInDb("CREATE (p:Product {code:'1367', name: 'Notebook', price: '199'})", DB_TEST_REL_WITH_COMPACT)
        val keyEnd = mapOf("ids" to mapOf("code" to "1367", "price" to "199"), "labels" to listOf("Product"))
        assertTopicFilled(kafkaConsumer) { it.count() == 1
                && JSONUtils.readValue<Map<*, *>>(it.first().key()) == keyEnd
        }

        // we create a relationship with start and end node with constraint
        runQueryInDb("MATCH (pe:Person:Other {name:'Sherlock'}), (pr:Product {name:'Notebook'}) MERGE (pe)-[:BUYS]->(pr)", DB_TEST_REL_WITH_COMPACT)
        val mapRel = mapOf("start" to keyStart, "end" to keyEnd, "label" to relType)
        assertTopicFilled(kafkaConsumer) { it.count() == 1
                && JSONUtils.readValue<Map<*, *>>(it.first().key()) == mapRel
        }

        // we update the relationship
        runQueryInDb("MATCH (:Person:Other {name:'Sherlock'})-[rel:BUYS]->(:Product {name:'Notebook'}) SET rel.orders = '100'", DB_TEST_REL_WITH_COMPACT)
        assertTopicFilled(kafkaConsumer) { it.count() == 1
                && JSONUtils.readValue<Map<*, *>>(it.first().key()) == mapRel
        }

        // we delete the relationship
        runQueryInDb("MATCH (:Person:Other {name:'Sherlock'})-[rel:BUYS]->(:Product {name:'Notebook'}) DELETE rel", DB_TEST_REL_WITH_COMPACT)
        assertTopicFilled(kafkaConsumer) { it.count() == 1
                && JSONUtils.readValue<Map<*, *>>(it.first().key()) == mapRel
        }
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