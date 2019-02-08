package streams.integrations

import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.*
import org.junit.rules.TestName
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.test.TestGraphDatabaseFactory
import org.testcontainers.containers.KafkaContainer
import streams.kafka.KafkaConfiguration
import streams.procedures.StreamsProcedures
import streams.serialization.JSONUtils
import kotlin.test.assertEquals

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
        @ClassRule @JvmField
        val kafka = KafkaContainer(confluentPlatformVersion)
    }

    lateinit var db: GraphDatabaseAPI

    private val WITH_REL_ROUTING_METHOD_SUFFIX = "WithRelRouting"
    private val WITH_NODE_ROUTING_METHOD_SUFFIX = "WithNodeRouting"
    private val MULTI_NODE_PATTERN_TEST: String = "MultiTopicPatternConfig"

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
        if (testName.methodName.endsWith(MULTI_NODE_PATTERN_TEST)) {
            graphDatabaseBuilder.setConfig("streams.source.topic.nodes.neo4j-product", "Product{name, code}")
                    .setConfig("streams.source.topic.nodes.neo4j-color", "Color{*}")
                    .setConfig("streams.source.topic.nodes.neo4j-basket", "Basket{*}")
                    .setConfig("streams.source.topic.relationships.neo4j-isin", "IS_IN{month,day}")
                    .setConfig("streams.source.topic.relationships.neo4j-hascolor", "HAS_COLOR{*}")
        }
        db = graphDatabaseBuilder.newGraphDatabase() as GraphDatabaseAPI
        db.dependencyResolver.resolveDependency(Procedures::class.java)
                .registerProcedure(StreamsProcedures::class.java, true)

    }

    @After
    fun tearDown() {
        db.shutdown()
    }

    @Test
    fun testCreateNode() {
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers,
                zookeeperConnect = kafka.envMap["KAFKA_ZOOKEEPER_CONNECT"]!!)
        val consumer = createConsumer(config)
        consumer.subscribe(listOf("neo4j"))
        db.execute("CREATE (:Person {name:'John Doe', age:42})").close()
        val records = consumer.poll(5000)
        assertEquals(1, records.count())
        assertEquals(true, records.all {
            JSONUtils.readValue<Map<String, Any>>(it.value()).let {
                var payload = it["payload"] as Map<String, Any?>
                val after = payload["after"] as Map<String, Any?>
                val labels = after["labels"] as List<String>
                val propertiesAfter = after["properties"] as Map<String, Any?>
                val meta = it["meta"] as Map<String, Any?>
                labels == listOf("Person") && propertiesAfter == mapOf("name" to "John Doe", "age" to 42) && meta["operation"] == "created"
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
            JSONUtils.readValue<Map<String, Any>>(it.value()).let {
                var payload = it["payload"] as Map<String, Any?>
                val after = payload["after"] as Map<String, Any?>
                val properties = after["properties"] as Map<String, Any?>
                payload["type"] == "relationship" && payload["label"] == "KNOWS" && properties["since"] == 2014
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
            JSONUtils.readValue<Map<String, Any>>(it.value()).let {
                var payload = it["payload"] as Map<String, Any?>
                val after = payload["after"] as Map<String, Any?>
                val labels = after["labels"] as List<String>
                val propertiesAfter = after["properties"] as Map<String, Any?>
                val meta = it["meta"] as Map<String, Any?>
                labels == listOf("Person") && propertiesAfter == mapOf("name" to "Andrea") && meta["operation"] == "created"
            }
        })
        consumer.close()
    }

    private fun createConsumer(config: KafkaConfiguration): KafkaConsumer<String, ByteArray> {
        val props = config.asProperties()
        props["group.id"] = "neo4j"
        props["enable.auto.commit"] = "true"
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = ByteArrayDeserializer::class.java
        props["auto.offset.reset"] = "earliest"
        return KafkaConsumer(props)
    }

    @Test
    fun testProcedure() {
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        val consumer = createConsumer(config)
        consumer.subscribe(listOf("neo4j"))
        val message = "Hello World"
        db.execute("CALL streams.publish('neo4j', '$message')").close()
        val records = consumer.poll(5000)
        assertEquals(1, records.count())
        assertEquals(true, records.all {
            JSONUtils.readValue<Map<String, Any>>(it.value()).let {
                val payload = it as Map<String, String>
                message == payload["payload"]
            }
        })
        consumer.close()
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

}