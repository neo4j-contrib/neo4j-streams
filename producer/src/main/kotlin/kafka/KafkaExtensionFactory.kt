package kafka

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
    val prefix = "kafka."

    override fun newInstance(kernelContext: KernelContext, dependencies: Dependencies): Lifecycle {
        val db = dependencies.graphdatabaseAPI()
        val log = dependencies.log()
        val configuration = dependencies.config()

        return object : LifecycleAdapter() {
            override fun start() {
                /*
                val userLog = log.getUserLog(KafkaModule::class.java)
                userLog.info("Initializing Kafka Connector")
                try {
                    val config = KafkaConfiguration.from(configuration.raw.filterKeys { it.startsWith(prefix) }.mapKeys { it.key.substring(prefix.length) })
                    val props = config.asProperties()

                    val producer = KafkaProducer<Long, ByteArray>(props)
                    userLog.info("Kafka initialization successful")
                    AdminClient.create(config.asProperties()).use { client -> client.createTopics(
                            config.topics.map { topic -> NewTopic(topic, config.partitionSize, config.replication.toShort()) }) }

                    val publisher = RecordPublisher(userLog, producer, config)

                    txHandler = db.registerTransactionEventHandler(KafkaModule(publisher))
                    userLog.info("Kafka Connector started.")
                } catch(e: Exception) {
                    userLog.error("Error initializing Kafka Connector", e)
                }
                */
            }

            override fun stop() {
                /*
                txHandler?.let { db.unregisterTransactionEventHandler<Any>(it) }
                */
            }
        }
    }

    interface Dependencies {
        fun graphdatabaseAPI(): GraphDatabaseAPI
        fun log(): LogService
        fun config(): Config
    }
}


