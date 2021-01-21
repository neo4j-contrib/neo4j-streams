package streams.integrations

import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.hamcrest.Matchers
import org.junit.*
import org.junit.Test
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
import kotlin.test.*


@Suppress("UNCHECKED_CAST", "DEPRECATION")
open class KafkaEventRouterBaseIT {

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
            Assume.assumeTrue("Kafka must be running", kafka.isRunning)
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
    open fun setUp() {
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

        db = graphDatabaseBuilder.newGraphDatabase() as GraphDatabaseAPI
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
}