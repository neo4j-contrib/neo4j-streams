package streams.integrations

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.config.TopicConfig
import org.hamcrest.Matchers
import org.junit.*
import org.neo4j.driver.SessionConfig
import streams.KafkaTestUtils
import streams.Neo4jContainerExtension
import streams.utils.StreamsUtils
import org.junit.Assume.assumeTrue
import org.neo4j.function.ThrowingSupplier
import streams.Assert.assertEventually
import streams.utils.JSONUtils
import streams.integrations.CompactionStrategyTestCommon.Companion.assertTopicFilled
import java.util.concurrent.TimeUnit

class KafkaEventRouterEnterpriseLogCompactionTSE {

    @Before
    fun setUp() {
        kafkaConsumer = KafkaTestUtils.createConsumer(bootstrapServers = KafkaEventRouterSuiteIT.kafka.bootstrapServers)
    }

    @After
    fun tearDown() {
        kafkaConsumer.close()
    }

    companion object {
        const val DB_ONE = "foo"
        const val DB_TWO = "bar"
        const val DB_THREE = "baz"

        const val TOPICS_ONE = "one"
        const val TOPICS_TWO = "two"
        const val TOPICS_THREE = "three"
        const val TOPICS_FOUR = "four"

        private var startedFromSuite = true
        lateinit var kafkaConsumer: KafkaConsumer<String, ByteArray>

        @JvmStatic
        private val neo4j = Neo4jContainerExtension()

        @BeforeClass
        @JvmStatic
        fun setUpContainer() {
            if (!KafkaEventRouterSuiteIT.isRunning) {
                startedFromSuite = false
                KafkaEventRouterSuiteIT.setUpContainer()
            }

            StreamsUtils.ignoreExceptions({
                neo4j.withKafka(KafkaEventRouterSuiteIT.kafka)
                        .withNeo4jConfig("streams.source.schema.polling.interval", "0")
                        .withNeo4jConfig("topic.discovery.polling.interval", "0")
                        .withNeo4jConfig("kafka.streams.log.compaction.strategy", TopicConfig.CLEANUP_POLICY_COMPACT)
                        .withNeo4jConfig("streams.source.topic.nodes.$TOPICS_ONE.from.$DB_ONE", "Person{*}")
                        .withNeo4jConfig("streams.source.topic.relationships.$TOPICS_ONE.from.$DB_ONE", "BUYS{*}")
                        .withNeo4jConfig("streams.source.topic.nodes.$TOPICS_TWO.from.$DB_ONE", "Product{*}")
                        .withNeo4jConfig("streams.source.topic.nodes.$TOPICS_THREE.from.$DB_TWO", "Person{*}")
                        .withNeo4jConfig("streams.source.topic.nodes.$TOPICS_FOUR.from.$DB_THREE", "Person{*}")
                        .withNeo4jConfig("streams.source.topic.relationships.$TOPICS_FOUR.from.$DB_THREE", "KNOWS{*}")

                neo4j.withDatabases(DB_ONE, DB_TWO, DB_THREE)
                neo4j.start()
                assumeTrue("Neo4j must be running", neo4j.isRunning)
            }, IllegalStateException::class.java)
        }

        fun createConstraintAndAssert(constraints: List<String>, db: String, size: Int = 0) {
            constraints.forEach { runQueryInDb(it, db) }

            assertEventually(ThrowingSupplier {
                runQueryInDb("call db.constraints()", db).list().size > size
            }, Matchers.equalTo(true), 60, TimeUnit.SECONDS)
        }

        @AfterClass
        @JvmStatic
        fun tearDownContainer() {
            neo4j.stop()
            if (!startedFromSuite) {
                KafkaEventRouterSuiteIT.tearDownContainer()
            }
        }

        private fun runQueryAndAssertIsFilled(query: String, db: String) {
            runQueryInDb(query, db)
            assertTopicFilled(kafkaConsumer)
        }

        private fun runQueryInDb(query: String, db: String) = neo4j.driver!!.session(SessionConfig.forDatabase(db)).run(query)

        fun createManyPersons(db: String) {
            runQueryInDb("UNWIND range(1, 999) AS id CREATE (:Person {name:id, surname: id})", db)
        }
    }

    @Test
    fun `node with node key constraint and topic compact`() {
        val queries = listOf("CREATE CONSTRAINT ON (p:Person) ASSERT (p.name, p.surname) IS NODE KEY")
        createConstraintAndAssert(queries, DB_TWO)

        kafkaConsumer.subscribe(listOf(TOPICS_THREE))

        // we create a node with constraint and check that key is equal to constraint
        runQueryInDb("CREATE (:Person {name:'Pippo', surname: 'Topolino'})", DB_TWO)

        var keyNode = mapOf("ids" to mapOf("surname" to "Topolino", "name" to "Pippo"), "labels" to listOf("Person"))
        assertTopicFilled(kafkaConsumer) { it.count() == 1 &&
                JSONUtils.readValue<Map<*, *>>(it.first().key()) == keyNode
        }

        keyNode = mapOf("ids" to mapOf("surname" to "Topolino", "name" to "Pluto"), "labels" to listOf("Person"))
        // we update the node
        runQueryInDb("MATCH (p:Person {name:'Pippo'}) SET p.name='Pluto'", DB_TWO)
        assertTopicFilled(kafkaConsumer) { it.count() == 1 &&
                JSONUtils.readValue<Map<*, *>>(it.first().key()) == keyNode
        }

        // we delete the node
        runQueryInDb("MATCH (p:Person {name:'Pluto'}) DETACH DELETE p", DB_TWO)
        assertTopicFilled(kafkaConsumer) { it.count() == 1 &&
                JSONUtils.readValue<Map<*, *>>(it.first().key()) == keyNode
        }
    }

    @Test
    fun `delete single tombstone relation with strategy compact and constraints`() {
        // we create a topic with strategy compact
        val keyRel = "KNOWS"
        val queries = listOf("CREATE CONSTRAINT ON (p:Person) ASSERT (p.name, p.surname) IS NODE KEY")
        createConstraintAndAssert(queries, DB_THREE)

        kafkaConsumer.subscribe(listOf(TOPICS_FOUR))

        runQueryAndAssertIsFilled("CREATE (:Person {name:'Pippo', surname: 'Pippo_2'})", DB_THREE)
        runQueryAndAssertIsFilled("CREATE (:Person {name:'Pluto', surname: 'Pluto_2'})", DB_THREE)
        runQueryAndAssertIsFilled("""
            |MATCH (pippo:Person {name:'Pippo'})
            |MATCH (pluto:Person {name:'Pluto'})
            |MERGE (pippo)-[:$keyRel]->(pluto)
        """.trimMargin(), DB_THREE)
        // we delete a rel, so will be created a tombstone record
        runQueryAndAssertIsFilled("MATCH (:Person {name:'Pippo'})-[rel:$keyRel]->(:Person {name:'Pluto'}) DELETE rel", DB_THREE)
        runQueryAndAssertIsFilled("CREATE (:Person {name:'Sherlock', surname: 'Holmes'})", DB_THREE)
        runQueryAndAssertIsFilled("MATCH (p:Person {name:'Sherlock'}) SET p.address = '221B Baker Street'", DB_THREE)
        // we delete a node, so will be created a tombstone record
        runQueryAndAssertIsFilled("MATCH (p:Person {name:'Sherlock'}) DETACH DELETE p", DB_THREE)
        runQueryAndAssertIsFilled("CREATE (:Person {name:'Watson', surname: 'John'})", DB_THREE)

        val mapStartExpected = mapOf("ids" to mapOf("name" to "Pippo", "surname" to "Pippo_2"), "labels" to listOf("Person"))
        val mapEndExpected = mapOf("ids" to mapOf("name" to "Pluto", "surname" to "Pluto_2"), "labels" to listOf("Person"))
        val nodeRecordExpected = mapOf("ids" to mapOf("surname" to "Holmes", "name" to "Sherlock"), "labels" to listOf("Person"))

        // to activate the log compaction process we create dummy messages and waiting for messages population
        createManyPersons(DB_THREE)
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
        val queries = listOf("CREATE CONSTRAINT ON (p:Product) ASSERT (p.code, p.price) IS NODE KEY",
                "CREATE CONSTRAINT ON (p:Other) ASSERT (p.address, p.city) IS NODE KEY")
        createConstraintAndAssert(queries, DB_ONE)

        kafkaConsumer.subscribe(listOf(TOPICS_ONE, TOPICS_TWO))

        runQueryInDb("CREATE (:Person:Other {name: 'Sherlock', surname: 'Holmes', address: 'Baker Street', city: 'London'})", DB_ONE)
        val keyStart = mapOf("ids" to mapOf("address" to "Baker Street", "city" to "London"), "labels" to listOf("Other", "Person"))
        assertTopicFilled(kafkaConsumer) { it.count() == 1
                && JSONUtils.readValue<Map<*, *>>(it.first().key()) == keyStart
        }

        runQueryInDb("CREATE (p:Product {code:'1367', name: 'Notebook', price: '199'})", DB_ONE)
        val keyEnd = mapOf("ids" to mapOf("code" to "1367", "price" to "199"), "labels" to listOf("Product"))
        assertTopicFilled(kafkaConsumer) { it.count() == 1
                && JSONUtils.readValue<Map<*, *>>(it.first().key()) == keyEnd
        }

        // we create a relationship with start and end node with constraint
        runQueryInDb("MATCH (pe:Person:Other {name:'Sherlock'}), (pr:Product {name:'Notebook'}) MERGE (pe)-[:BUYS]->(pr)", DB_ONE)
        val mapRel = mapOf("start" to keyStart, "end" to keyEnd, "label" to relType)
        assertTopicFilled(kafkaConsumer) { it.count() == 1
                && JSONUtils.readValue<Map<*, *>>(it.first().key()) == mapRel
        }

        // we update the relationship
        runQueryInDb("MATCH (:Person:Other {name:'Sherlock'})-[rel:BUYS]->(:Product {name:'Notebook'}) SET rel.orders = '100'", DB_ONE)
        assertTopicFilled(kafkaConsumer) { it.count() == 1
                && JSONUtils.readValue<Map<*, *>>(it.first().key()) == mapRel
        }

        // we delete the relationship
        runQueryInDb("MATCH (:Person:Other {name:'Sherlock'})-[rel:BUYS]->(:Product {name:'Notebook'}) DELETE rel", DB_ONE)
        assertTopicFilled(kafkaConsumer) { it.count() == 1
                && JSONUtils.readValue<Map<*, *>>(it.first().key()) == mapRel
        }
    }
}
