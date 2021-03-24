package streams.integrations

import extension.newDatabase
import org.junit.After
import org.junit.AfterClass
import org.junit.Assume
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.rules.TestName
import org.neo4j.graphdb.factory.GraphDatabaseBuilder
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.test.TestGraphDatabaseFactory
import org.testcontainers.containers.KafkaContainer
import streams.events.OperationType
import streams.events.StreamsTransactionEvent
import streams.procedures.StreamsProcedures
import streams.utils.StreamsUtils


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
            Assume.assumeTrue("Kafka must be running", this::kafka.isInitialized && kafka.isRunning)
        }

        @AfterClass @JvmStatic
        fun tearDownContainer() {
            StreamsUtils.ignoreExceptions({
                kafka.stop()
            }, UninitializedPropertyAccessException::class.java)
        }

        // common methods
        fun commonRelAssertions(value: StreamsTransactionEvent) = value.payload.before == null
                && value.payload.after!!.properties!!.isNullOrEmpty()
                && value.schema.properties == emptyMap<String, String>()
                && value.meta.operation == OperationType.created

        fun commonRelAssertionsUpdate(value: StreamsTransactionEvent) = value.payload.before!!.properties!!.isNullOrEmpty()
                && value.payload.after!!.properties == mapOf("type" to "update")
                && value.schema.properties == mapOf("type" to "String")
                && value.meta.operation == OperationType.updated

        fun commonRelAssertionsDelete(value: StreamsTransactionEvent) = value.payload.before!!.properties == mapOf("type" to "update")
                && value.payload.after == null
                && value.schema.properties == mapOf("type" to "String")
                && value.meta.operation == OperationType.deleted
    }

    lateinit var db: GraphDatabaseAPI

    lateinit var graphDatabaseBuilder: GraphDatabaseBuilder

    @Rule
    @JvmField
    var testName = TestName()

    @Before
    open fun setUp() {
        graphDatabaseBuilder = TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig("kafka.bootstrap.servers", kafka.bootstrapServers)

        db = graphDatabaseBuilder.newDatabase() as GraphDatabaseAPI
        db.dependencyResolver.resolveDependency(Procedures::class.java)
                .registerProcedure(StreamsProcedures::class.java, true)
    }

    @After
    fun tearDown() {
        db.shutdown()
    }
}