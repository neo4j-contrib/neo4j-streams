package streams.integrations

import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import extension.newDatabase
import org.hamcrest.Matchers
import org.junit.*
import org.junit.rules.TestName
import org.neo4j.function.ThrowingSupplier
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.test.TestGraphDatabaseFactory
import org.testcontainers.containers.KafkaContainer
import streams.events.*
import streams.kafka.KafkaConfiguration
import streams.kafka.KafkaTestUtils.createConsumer
import streams.procedures.StreamsProcedures
import streams.serialization.JSONUtils
import streams.utils.StreamsUtils
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.assertNotNull

@Suppress("UNCHECKED_CAST", "DEPRECATION")
class KafkaEventRouterIT {

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
                kafka.start()
                exists = true
            }, IllegalStateException::class.java)
            Assume.assumeTrue("Kafka container has to exist", exists)
            Assume.assumeTrue("Kafka must be running", this::kafka.isInitialized && kafka.isRunning)
        }

        @AfterClass @JvmStatic
        fun tearDownContainer() {
            StreamsUtils.ignoreExceptions({
                kafka.stop()
            }, UninitializedPropertyAccessException::class.java)
        }
    }

    lateinit var db: GraphDatabaseAPI

    private val WITH_REL_ROUTING_METHOD_SUFFIX = "WithRelRouting"
    private val WITH_NODE_ROUTING_METHOD_SUFFIX = "WithNodeRouting"
    private val MULTI_NODE_PATTERN_TEST_SUFFIX = "MultiTopicPatternConfig"
    private val WITH_CONSTRAINTS_SUFFIX = "WithConstraints"
    private val WITH_TEST_DELETE_TOPIC = "WithTestDeleteTopic"

    @Rule
    @JvmField
    var testName = TestName()

    @Before
    fun setUp() {
        var graphDatabaseBuilder = TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig("kafka.bootstrap.servers", kafka.bootstrapServers)
        if (testName.methodName.endsWith(WITH_REL_ROUTING_METHOD_SUFFIX)) {
            graphDatabaseBuilder.setConfig("streams.source.topic.relationships.knows", "KNOWS{*}")
        }
        if (testName.methodName.endsWith(WITH_NODE_ROUTING_METHOD_SUFFIX)) {
            graphDatabaseBuilder.setConfig("streams.source.topic.nodes.person", "Person{*}")
        }
        if (testName.methodName.endsWith(MULTI_NODE_PATTERN_TEST_SUFFIX)) {
            graphDatabaseBuilder.setConfig("streams.source.topic.nodes.neo4j-product", "Product{name, code}")
                    .setConfig("streams.source.topic.nodes.neo4j-color", "Color{*}")
                    .setConfig("streams.source.topic.nodes.neo4j-basket", "Basket{*}")
                    .setConfig("streams.source.topic.relationships.neo4j-isin", "IS_IN{month,day}")
                    .setConfig("streams.source.topic.relationships.neo4j-hascolor", "HAS_COLOR{*}")
        }
        if (testName.methodName.endsWith(WITH_CONSTRAINTS_SUFFIX)) {
            graphDatabaseBuilder.setConfig("streams.source.topic.nodes.personConstraints", "PersonConstr{*}")
                    .setConfig("streams.source.topic.nodes.productConstraints", "ProductConstr{*}")
                    .setConfig("streams.source.topic.relationships.boughtConstraints", "BOUGHT{*}")
                    .setConfig("streams.source.schema.polling.interval", "0")
        }
        if (testName.methodName.endsWith(WITH_TEST_DELETE_TOPIC)) {
            graphDatabaseBuilder.setConfig("streams.source.topic.nodes.testdeletetopic", "Person:Neo4j{*}")
                    .setConfig("streams.source.topic.relationships.testdeletetopic", "KNOWS{*}")
        }
        db = graphDatabaseBuilder.newDatabase() as GraphDatabaseAPI
        db.dependencyResolver.resolveDependency(Procedures::class.java)
                .registerProcedure(StreamsProcedures::class.java, true)
        if (testName.methodName.endsWith(WITH_CONSTRAINTS_SUFFIX)) {
            db.execute("CREATE CONSTRAINT ON (p:PersonConstr) ASSERT p.name IS UNIQUE").close()
            db.execute("CREATE CONSTRAINT ON (p:ProductConstr) ASSERT p.name IS UNIQUE").close()
        }
    }

    @After
    fun tearDown() {
        db.shutdown()
    }

    @Test
    fun testCreateNode() {
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        val consumer = createConsumer(config)
        consumer.subscribe(listOf("neo4j"))
        db.execute("CREATE (:Person {name:'John Doe', age:42})").close()
        val records = consumer.poll(5000)
        assertEquals(1, records.count())
        assertEquals(true, records.all {
            JSONUtils.asStreamsTransactionEvent(it.value()).let {
                val after = it.payload.after as NodeChange
                val labels = after.labels
                val propertiesAfter = after.properties
                labels == listOf("Person") && propertiesAfter == mapOf("name" to "John Doe", "age" to 42)
                        && it.meta.operation == OperationType.created
                        && it.schema.properties == mapOf("name" to "String", "age" to "Long")
                        && it.schema.constraints.isEmpty()
            }
        })
        consumer.close()
    }

    @Test
    fun testCreateRelationshipWithRelRouting() {
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        val consumer = createConsumer(config)
        consumer.subscribe(listOf("knows"))
        db.execute("CREATE (:Person {name:'Andrea'})-[:KNOWS{since: 2014}]->(:Person {name:'Michael'})").close()
        val records = consumer.poll(5000)
        assertEquals(1, records.count())
        assertEquals(true, records.all {
            JSONUtils.asStreamsTransactionEvent(it.value()).let {
                var payload = it.payload as RelationshipPayload
                val properties = payload.after!!.properties!!
                payload.type == EntityType.relationship && payload.label == "KNOWS"
                        && properties["since"] == 2014
                        && it.schema.properties == mapOf("since" to "Long")
                        && it.schema.constraints.isEmpty()
            }
        })
        consumer.close()
    }

    @Test
    fun testCreateNodeWithNodeRouting() {
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        val consumer = createConsumer(config)
        consumer.subscribe(listOf("person"))
        db.execute("CREATE (:Person {name:'Andrea'})").close()
        val records = consumer.poll(5000)
        assertEquals(1, records.count())
        assertEquals(true, records.all {
            JSONUtils.asStreamsTransactionEvent(it.value()).let {
                val payload = it.payload as NodePayload
                val labels = payload.after!!.labels!!
                val properties = payload.after!!.properties
                labels == listOf("Person") && properties == mapOf("name" to "Andrea")
                        && it.meta.operation == OperationType.created
                        && it.schema.properties == mapOf("name" to "String")
                        && it.schema.constraints.isEmpty()
            }
        })
        consumer.close()
    }

    @Test
    fun testProcedure() {
        val topic = UUID.randomUUID().toString()
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        val consumer = createConsumer(config)
        consumer.subscribe(listOf(topic))
        val message = "Hello World"
        db.execute("CALL streams.publish('$topic', '$message')").close()
        val records = consumer.poll(5000)
        assertEquals(1, records.count())
        assertTrue { records.all {
            JSONUtils.readValue<StreamsEvent>(it.value()).let {
                message == it.payload
            }
        }}
        consumer.close()
    }
    
    @Test
    fun testProcedureWithKey() {
        val topic = UUID.randomUUID().toString()
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        createConsumer(config).use {
            it.subscribe(listOf(topic))
            val message = "Hello World"
            val keyRecord = "test"
            db.execute("CALL streams.publish('$topic', '$message', {key: '$keyRecord'} )").close()
            val records = it.poll(5000)
            assertEquals(1, records.count())
            assertTrue { records.all {
                JSONUtils.readValue<StreamsEvent>(it.value()).payload == message
                        && JSONUtils.readValue<String>(it.key()) == keyRecord
            }}
        }
    }

    @Test
    fun testProcedureWithKeyAsMap() {
        val topic = UUID.randomUUID().toString()
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        createConsumer(config).use {
            it.subscribe(listOf(topic))
            val message = "Hello World"
            val keyRecord = mutableMapOf("one" to "Foo", "two" to "Baz", "three" to "Bar")
            db.execute("CALL streams.publish('$topic', '$message', {key: \$key } )", mapOf("key" to keyRecord)).close()
            val records = it.poll(5000)
            assertEquals(1, records.count())
            assertTrue { records.all {
                JSONUtils.readValue<StreamsEvent>(it.value()).payload == message
                        && JSONUtils.readValue<Map<String, String>>(it.key()) == keyRecord
            }}
        }
    }

    @Test
    fun testProcedureWithPartitionAsNotNumber() {
        val topic = UUID.randomUUID().toString()
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        createConsumer(config).use {
            it.subscribe(listOf(topic))
            val message = "Hello World"
            val keyRecord = "test"
            val partitionRecord = "notNumber"
            assertFailsWith(QueryExecutionException::class) {
                db.execute("CALL streams.publish('$topic', '$message', {key: '$keyRecord', partition: '$partitionRecord' })").close()
            }
        }
    }

    @Test
    fun testProcedureWithPartitionAndKey() {
        val topic = UUID.randomUUID().toString()
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        createConsumer(config).use {
            it.subscribe(listOf(topic))
            val message = "Hello World"
            val keyRecord = "test"
            val partitionRecord = 0
            db.execute("CALL streams.publish('$topic', '$message', {key: '$keyRecord', partition: $partitionRecord })").close()
            val records = it.poll(5000)
            assertEquals(1, records.count())
            assertTrue{ records.all {
                JSONUtils.readValue<StreamsEvent>(it.value()).payload == message
                        && JSONUtils.readValue<String>(it.key()) == keyRecord
                        && partitionRecord == it.partition()
            }}
        }
    }

    @Test
    fun testCantPublishNull() {
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        createConsumer(config).use { consumer ->
            consumer.subscribe(listOf("neo4j"))

            assertFailsWith(RuntimeException::class) {
                db.execute("CALL streams.publish('neo4j', null)")
            }
        }
    }

    @Test
    fun testProcedureSyncWithNode() {
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        createConsumer(config).use { consumer ->

            consumer.subscribe(listOf("neo4j"))
            db.execute("MATCH (n:Baz) DETACH DELETE n").close()
            db.execute("CREATE (n:Baz {age: 23, name: 'Foo', surname: 'Bar'})").close()

            val recordsCreation = consumer.poll(5000)
            assertEquals(1, recordsCreation.count())

            db.execute("MATCH (n:Baz) \n" +
                    "CALL streams.publish.sync('neo4j', n) \n" +
                    "YIELD value \n" +
                    "RETURN value").use { result ->
                assertTrue { result.hasNext() }
                val resultMap = (result.next())["value"] as Map<String, Any>

                assertNotNull(resultMap["offset"])
                assertNotNull(resultMap["partition"])
                assertNotNull(resultMap["keySize"])
                assertNotNull(resultMap["valueSize"])
                assertNotNull(resultMap["timestamp"])

                assertFalse { result.hasNext() }
            }

            val records = consumer.poll(5000)
            assertEquals(1, records.count())
            assertEquals(3, ((records.map {
                JSONUtils.readValue<StreamsEvent>(it.value()).payload
            }[0] as Map<String, Any>)["properties"] as Map<String, Any>).size)
        }
    }

    @Test
    fun testProcedureSync() {
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        createConsumer(config).use { consumer ->
            consumer.subscribe(listOf("syncTopic"))
            val message = "Hello World"
            db.execute("CALL streams.publish.sync('syncTopic', '$message')").use { result ->
                assertTrue { result.hasNext() }
                val resultMap = (result.next())["value"] as Map<String, Any>
                assertNotNull(resultMap["offset"])
                assertNotNull(resultMap["partition"])
                assertNotNull(resultMap["keySize"])
                assertNotNull(resultMap["valueSize"])
                assertNotNull(resultMap["timestamp"])

                assertFalse { result.hasNext() }
            }

            val records = consumer.poll(5000)
            assertEquals(1, records.count())
            assertEquals(true, records.all {
                JSONUtils.readValue<StreamsEvent>(it.value()).payload == message
            })
        }
    }

    private fun getRecordCount(config: KafkaConfiguration, topic: String): Int {
        val consumer = createConsumer(config)
        consumer.subscribe(listOf(topic))
        val count = consumer.poll(5000).count()
        consumer.close()
        return count
    }

    @Test
    fun testMultiTopicPatternConfig() = runBlocking {
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        db.execute("""
            CREATE (p:Product{id: "A1", code: "X1", name: "Name X1", price: 1000})-[:IS_IN{month:4, day:4, year:2018}]->(b:Basket{name:"Basket-A", created: "20181228"}),
	            (p)-[:HAS_COLOR]->(c:Color{name: "Red"})
        """.trimIndent()).close()

        val recordsProduct = async { getRecordCount(config, "neo4j-product") }
        val recordsColor = async { getRecordCount(config, "neo4j-color") }
        val recordsBasket = async { getRecordCount(config, "neo4j-basket") }
        val recordsIsIn = async { getRecordCount(config, "neo4j-isin") }
        val recordsHasColor = async { getRecordCount(config, "neo4j-hascolor") }

        assertEquals(1, recordsProduct.await())
        assertEquals(1, recordsColor.await())
        assertEquals(1, recordsBasket.await())
        assertEquals(1, recordsIsIn.await())
        assertEquals(1, recordsHasColor.await())
    }

    @Test
    fun testCreateNodeWithConstraints() {
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        val consumer = createConsumer(config)
        consumer.subscribe(listOf("personConstraints"))
        db.execute("CREATE (:PersonConstr {name:'Andrea'})").close()
        val records = consumer.poll(5000)
        assertEquals(1, records.count())
        assertEquals(true, records.all {
            JSONUtils.asStreamsTransactionEvent(it.value()).let {
                val payload = it.payload as NodePayload
                val labels = payload.after!!.labels!!
                val properties = payload.after!!.properties
                labels == listOf("PersonConstr") && properties == mapOf("name" to "Andrea")
                        && it.meta.operation == OperationType.created
                        && it.schema.properties == mapOf("name" to "String")
                        && it.schema.constraints == listOf(Constraint("PersonConstr", setOf("name"), StreamsConstraintType.UNIQUE))
            }
        })
        consumer.close()
    }

    @Test
    fun testCreateRelationshipWithConstraints() {
        db.execute("CREATE (:PersonConstr {name:'Andrea'})").close()
        db.execute("CREATE (:ProductConstr {name:'My Awesome Product', price: '100€'})").close()
        db.execute("""
            |MATCH (p:PersonConstr {name:'Andrea'})
            |MATCH (pp:ProductConstr {name:'My Awesome Product'})
            |MERGE (p)-[:BOUGHT]->(pp)
        """.trimMargin())
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        val consumer = createConsumer(config)
        consumer.subscribe(listOf("personConstraints", "productConstraints", "boughtConstraints"))
        val records = consumer.poll(10000)
        assertEquals(3, records.count())

        val map = records
                .map {
                    val evt = JSONUtils.asStreamsTransactionEvent(it.value())
                    evt.payload.type to evt
                }
                .groupBy({ it.first }, { it.second })
        assertEquals(true, map[EntityType.node].orEmpty().isNotEmpty() && map[EntityType.node].orEmpty().all {
            val payload = it.payload as NodePayload
            val (labels, properties) = payload.after!!.labels!! to payload.after!!.properties!!
            when (labels) {
                listOf("ProductConstr") -> properties == mapOf("name" to "My Awesome Product", "price" to "100€")
                        && it.meta.operation == OperationType.created
                        && it.schema.properties == mapOf("name" to "String", "price" to "String")
                        && it.schema.constraints == listOf(Constraint("ProductConstr", setOf("name"), StreamsConstraintType.UNIQUE))
                listOf("PersonConstr") -> properties == mapOf("name" to "Andrea")
                        && it.meta.operation == OperationType.created
                        && it.schema.properties == mapOf("name" to "String")
                        && it.schema.constraints == listOf(Constraint("PersonConstr", setOf("name"), StreamsConstraintType.UNIQUE))
                else -> false
            }
        })
        assertEquals(true, map[EntityType.relationship].orEmpty().isNotEmpty() && map[EntityType.relationship].orEmpty().all {
            val payload = it.payload as RelationshipPayload
            val (start, end, properties) = Triple(payload.start, payload.end, payload.after!!.properties!!)
            properties.isNullOrEmpty()
                    && start.ids == mapOf("name" to "Andrea")
                    && end.ids == mapOf("name" to "My Awesome Product")
                    && it.meta.operation == OperationType.created
                    && it.schema.properties == emptyMap<String, String>()
                    && it.schema.constraints.toSet() == setOf(
                            Constraint("PersonConstr", setOf("name"), StreamsConstraintType.UNIQUE),
                            Constraint("ProductConstr", setOf("name"), StreamsConstraintType.UNIQUE))
        })
        consumer.close()
    }

    @Test
    fun testDeleteNodeWithTestDeleteTopic() {
        val topic = "testdeletetopic"
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        val kafkaConsumer = createConsumer(config)
        kafkaConsumer.subscribe(listOf(topic))
        db.execute("CREATE (:Person:ToRemove {name:'John Doe', age:42})-[:KNOWS]->(:Person {name:'Jane Doe', age:36})")
        org.neo4j.test.assertion.Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            kafkaConsumer.poll(5000).count() > 0
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
        db.execute("MATCH (p:Person {name:'John Doe', age:42}) REMOVE p:ToRemove")
        org.neo4j.test.assertion.Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            kafkaConsumer.poll(5000)
                    .map { JSONUtils.asStreamsTransactionEvent(it.value()) }
                    .filter { it.meta.operation == OperationType.updated }
                    .count() > 0
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
        db.execute("MATCH (p:Person) DETACH DELETE p")
        val count = db.execute("MATCH (p:Person {name:'John Doe', age:42}) RETURN count(p) AS count")
                .columnAs<Long>("count").next()
        assertEquals(0, count)
        org.neo4j.test.assertion.Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            kafkaConsumer.poll(5000)
                    .map { JSONUtils.asStreamsTransactionEvent(it.value()) }
                    .filter { it.meta.operation == OperationType.deleted }
                    .count() > 0
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
        kafkaConsumer.close()
    }

    @Test
    fun testProcedureSyncWithKeyNull() {
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        createConsumer(config).use { consumer ->

            val message = "Hello World"
            consumer.subscribe(listOf("neo4j"))
            db.execute("CREATE (n:Foo {id: 1, name: 'Bar'})").close()

            val recordsCreation = consumer.poll(5000)
            assertEquals(1, recordsCreation.count())

            db.execute("MATCH (n:Foo {id: 1}) CALL streams.publish.sync('neo4j', '$message', {key: n.foo}) YIELD value RETURN value").use { result ->
                assertTrue { result.hasNext() }
                val resultMap = (result.next())["value"] as Map<String, Any>

                assertNotNull(resultMap["offset"])
                assertNotNull(resultMap["partition"])
                assertNotNull(resultMap["keySize"])
                assertNotNull(resultMap["valueSize"])
                assertNotNull(resultMap["timestamp"])

                assertFalse { result.hasNext() }
            }

            val records = consumer.poll(5000)
            assertEquals(1, records.count())
            assertTrue { records.all {
                JSONUtils.readValue<StreamsEvent>(it.value()).payload == message
                        && it.key() == null
            }}
        }
    }

    @Test
    fun testProcedureSyncWithConfig() {
        AdminClient.create(mapOf("bootstrap.servers" to kafka.bootstrapServers)).use {
            val topic = UUID.randomUUID().toString()
            it.createTopics(listOf(NewTopic(topic, 5, 1)))
            val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
            createConsumer(config).use { consumer ->
                consumer.subscribe(listOf(topic))
                val message = "Hello World"
                val keyRecord = "test"
                val partitionRecord = 1
                db.execute("CALL streams.publish.sync('$topic', '$message', {key: '$keyRecord', partition: $partitionRecord })").use {result ->
                    assertTrue { result.hasNext() }
                    val resultMap = (result.next())["value"] as Map<String, Any>
                    assertNotNull(resultMap["offset"])
                    assertEquals(partitionRecord, resultMap["partition"])
                    assertNotNull(resultMap["keySize"])
                    assertNotNull(resultMap["valueSize"])
                    assertNotNull(resultMap["timestamp"])
                    assertFalse { result.hasNext() }
                }
                val records = consumer.poll(5000)
                assertEquals(1, records.count())
                assertEquals(1, records.count { it.partition() == 1 })
                assertTrue{ records.all {
                    JSONUtils.readValue<StreamsEvent>(it.value()).payload == message
                            && JSONUtils.readValue<String>(it.key()) == keyRecord
                            && partitionRecord == it.partition()
                }}
            }
        }
    }

    @Test
    fun testProcedureWithTopicWithMultiplePartitionAndKey() {
        AdminClient.create(mapOf("bootstrap.servers" to kafka.bootstrapServers)).use {
            val topic = UUID.randomUUID().toString()
            it.createTopics(listOf(NewTopic(topic, 3, 1)))
            val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
            createConsumer(config).use { consumer ->
                consumer.subscribe(listOf(topic))
                val message = "Hello World"
                val keyRecord = "test"
                val partitionRecord = 2
                db.execute("CALL streams.publish('$topic', '$message', {key: '$keyRecord', partition: $partitionRecord })")
                val records = consumer.poll(5000)
                assertEquals(1, records.count())
                assertEquals(1, records.count { it.partition() == 2 })
                assertTrue{ records.all {
                    JSONUtils.readValue<StreamsEvent>(it.value()).payload == message
                            && JSONUtils.readValue<String>(it.key()) == keyRecord
                            && partitionRecord == it.partition()
                }}
            }
        }
    }

    @Test
    fun testProcedureSendMessageToNotExistentPartition() {
        AdminClient.create(mapOf("bootstrap.servers" to kafka.bootstrapServers)).use {
            val topic = UUID.randomUUID().toString()
            it.createTopics(listOf(NewTopic(topic, 3, 1)))
            val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
            createConsumer(config).use { consumer ->
                consumer.subscribe(listOf(topic))
                val message = "Hello World"
                val keyRecord = "test"
                val partitionRecord = 9
                db.execute("CALL streams.publish('$topic', '$message', {key: '$keyRecord', partition: $partitionRecord })")
                val records = consumer.poll(5000)
                assertEquals(0, records.count())
            }
        }
    }
}