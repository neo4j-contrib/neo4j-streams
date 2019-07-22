package integrations.kafka

import org.junit.AfterClass
import org.junit.Assume.assumeTrue
import org.junit.BeforeClass
import org.junit.runner.RunWith
import org.junit.runners.Suite
import org.testcontainers.containers.KafkaContainer
import streams.utils.StreamsUtils

@RunWith(Suite::class)
@Suite.SuiteClasses(
        KafkaEventSinkCDC::class,
        KafkaEventSinkCommit::class,
        KafkaEventSinkDLQ::class,
        KafkaEventSinkPattern::class,
        KafkaEventSinkSimple::class,
        KafkaStreamsSinkProcedures::class
)
class KafkaEventSinkSuiteIT {
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
        @JvmStatic lateinit var kafka: KafkaContainer

        var isRunning = false

        @BeforeClass
        @JvmStatic
        fun setUpContainer() {
            StreamsUtils.ignoreExceptions({
                kafka = KafkaContainer(confluentPlatformVersion)
                kafka.start()
                isRunning = true
            }, IllegalStateException::class.java)
            assumeTrue("Kafka container has to exist", isRunning)
            assumeTrue("Kafka must be running", kafka.isRunning)
        }

        @AfterClass
        @JvmStatic
        fun tearDownContainer() {
            StreamsUtils.ignoreExceptions({
                kafka.stop()
            }, kotlin.UninitializedPropertyAccessException::class.java)
        }
    }
}