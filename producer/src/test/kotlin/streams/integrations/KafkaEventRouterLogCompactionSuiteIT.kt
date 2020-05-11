package streams.integrations

import org.junit.AfterClass
import org.junit.Assume
import org.junit.BeforeClass
import org.junit.runner.RunWith
import org.junit.runners.Suite
import org.testcontainers.containers.KafkaContainer
import streams.utils.StreamsUtils

@RunWith(Suite::class)
@Suite.SuiteClasses(
        KafkaEventRouterLogCompactionTSE::class
)
class KafkaEventRouterLogCompactionSuiteIT {

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
        private const val confluentPlatformVersion = "5.3.1-1"
        @JvmStatic
        lateinit var kafka: KafkaContainer

        var isRunning = false

        @BeforeClass @JvmStatic
        fun setUpContainer() {
            var exists = false
            StreamsUtils.ignoreExceptions({
                kafka = KafkaContainer(confluentPlatformVersion)
                kafka.addEnv("KAFKA_LOG_CLEANUP_POLICY", "compact")
                kafka.start()
                exists = true
            }, IllegalStateException::class.java)
            Assume.assumeTrue("Kafka container has to exist", exists)
            Assume.assumeTrue("Kafka must be running", Companion::kafka.isInitialized && kafka.isRunning)
        }

        @AfterClass @JvmStatic
        fun tearDownContainer() {
            StreamsUtils.ignoreExceptions({
                kafka.stop()
            }, UninitializedPropertyAccessException::class.java)
        }
    }
}