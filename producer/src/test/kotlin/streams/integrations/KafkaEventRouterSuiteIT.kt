package streams.integrations

import org.junit.AfterClass
import org.junit.Assume
import org.junit.BeforeClass
import org.junit.runner.RunWith
import org.junit.runners.Suite
import org.neo4j.kernel.api.procedure.GlobalProcedures
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.Network
import streams.KafkaVersion
import streams.procedures.StreamsProcedures
import streams.utils.StreamsUtils

@RunWith(Suite::class)
@Suite.SuiteClasses(
        KafkaEventRouterProcedureTSE::class,
        KafkaEventRouterSimpleTSE::class,
        KafkaEventRouterWithConstraintsTSE::class,
        KafkaEventRouterEnterpriseTSE::class,
        KafkaEventRouterCompactionStrategyTSE::class
)
class KafkaEventRouterSuiteIT {

    companion object {
        private const val confluentPlatformVersion = KafkaVersion.CURRENT
        @JvmStatic
        lateinit var kafka: KafkaContainer

        var isRunning = false

        @BeforeClass @JvmStatic
        fun setUpContainer() {
            StreamsUtils.ignoreExceptions({
                kafka = KafkaContainer(confluentPlatformVersion)
                    .withNetwork(Network.newNetwork())
                kafka.start()
                isRunning = kafka.isRunning
            }, IllegalStateException::class.java)
            Assume.assumeTrue("Kafka container has to exist", isRunning)
            Assume.assumeTrue("Kafka must be running", ::kafka.isInitialized && kafka.isRunning)
        }

        @AfterClass @JvmStatic
        fun tearDownContainer() {
            StreamsUtils.ignoreExceptions({
                kafka.stop()
            }, UninitializedPropertyAccessException::class.java)
        }

        fun registerPublishProcedure(db: GraphDatabaseAPI) {
            db.dependencyResolver.resolveDependency(GlobalProcedures::class.java)
                    .registerProcedure(StreamsProcedures::class.java, true)
        }
    }

}