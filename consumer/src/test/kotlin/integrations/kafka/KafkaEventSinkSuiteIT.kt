package integrations.kafka

import org.junit.AfterClass
import org.junit.Assume
import org.junit.Assume.assumeTrue
import org.junit.BeforeClass
import org.junit.runner.RunWith
import org.junit.runners.Suite
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.Network
import streams.KafkaVersion
import streams.MavenUtils
import streams.utils.StreamsUtils

@RunWith(Suite::class)
@Suite.SuiteClasses(
        KafkaEventSinkCDCTSE::class,
        KafkaEventSinkCommitTSE::class,
        KafkaEventSinkDLQTSE::class,
        KafkaEventSinkPatternTSE::class,
        KafkaEventSinkSimpleTSE::class,
        KafkaStreamsSinkProceduresTSE::class,
        KafkaEventSinkCUDFormatTSE::class,
        KafkaEventSinkAvroTSE::class,
        KafkaNeo4jRecoveryTSE::class,
        KafkaEventSinkEnterpriseTSE::class
)
class KafkaEventSinkSuiteIT {
    companion object {
       private const val confluentPlatformVersion = KafkaVersion.CURRENT
        @JvmStatic lateinit var kafka: KafkaContainer
        @JvmStatic lateinit var schemaRegistry: SchemaRegistryContainer

        var isRunning = false

        @BeforeClass
        @JvmStatic
        fun setUpContainer() {
            StreamsUtils.ignoreExceptions({
                kafka = KafkaContainer(confluentPlatformVersion)
                    .withNetwork(Network.newNetwork())
                schemaRegistry = SchemaRegistryContainer(confluentPlatformVersion)
                        .withKafka(kafka)
                kafka.start()
                schemaRegistry.start()
                isRunning = true
            }, IllegalStateException::class.java)
            assumeTrue("Kafka must be running", ::kafka.isInitialized && kafka.isRunning)
            assumeTrue("Schema Registry must be running", schemaRegistry.isRunning)
            assumeTrue("isRunning must be true", isRunning)
        }

        @AfterClass
        @JvmStatic
        fun tearDownContainer() {
            StreamsUtils.ignoreExceptions({
                kafka.stop()
                schemaRegistry.stop()
                isRunning = false
            }, UninitializedPropertyAccessException::class.java)
        }
    }
}