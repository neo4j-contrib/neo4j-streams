package kafka

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.neo4j.graphdb.event.TransactionEventHandler
import org.neo4j.helpers.Service
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.extension.KernelExtensionFactory
import org.neo4j.kernel.impl.logging.LogService
import org.neo4j.kernel.impl.spi.KernelContext
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.kernel.lifecycle.LifecycleAdapter

@Service.Implementation(KernelExtensionFactory::class)
class KafkaExtensionFactory : KernelExtensionFactory<KafkaExtensionFactory.Dependencies>("KAFKA") {
    private var txHandler: TransactionEventHandler<Any>? = null

    override fun newInstance(kernelContext: KernelContext, dependencies: Dependencies): Lifecycle {
        val db = dependencies.graphdatabaseAPI()
        val log = dependencies.log()

        return object : LifecycleAdapter() {
            override fun start() {
                val userLog = log.getUserLog(KafkaModule::class.java)
                userLog.info("Starting up")
                try {
                    val neo4jConfig = db.dependencyResolver.resolveDependency(Config::class.java)
                    val config = KafkaConfiguration(neo4jConfig.raw)
                    val props = config.asProperties()
                    AdminClient.create(props).use { it.createTopics(listOf(NewTopic(config.topic, config.partitionSize, config.replication.toShort()))) }

                    val producer = KafkaProducer<Long, ByteArray>(props)
                    userLog.info("Kafka initialization successful")

                    txHandler = db.registerTransactionEventHandler(KafkaModule(userLog, producer, config))
                } catch(e: Exception) {
                    userLog.error("Error initializing Kafka Integration", e)
                }
            }

            override fun stop() {
                txHandler?.let { db.unregisterTransactionEventHandler<Any>(it) }
            }
        }
    }

    interface Dependencies {
        fun graphdatabaseAPI(): GraphDatabaseAPI
        fun log(): LogService
    }
}


