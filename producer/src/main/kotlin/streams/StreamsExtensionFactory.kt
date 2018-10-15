package streams

import org.neo4j.helpers.Service
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.extension.KernelExtensionFactory
import org.neo4j.kernel.impl.logging.LogService
import org.neo4j.kernel.impl.spi.KernelContext
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.kernel.lifecycle.LifecycleAdapter

@Service.Implementation(KernelExtensionFactory::class)
class StreamsExtensionFactory : KernelExtensionFactory<StreamsExtensionFactory.Dependencies>("Streams") {
    private var txHandler: StreamsTransactionEventHandler? = null

    override fun newInstance(context: KernelContext, dependencies: Dependencies): Lifecycle {
        val db = dependencies.graphdatabaseAPI()
        val log = dependencies.log()
        val configuration = dependencies.config()

        return object : LifecycleAdapter() {

            override fun start() {
                var streamsLog = log.getUserLog(StreamsExtensionFactory::class.java)
                try {
                    val streamHandler = StreamsEventRouterFactory.getStreamsEventRouter(log, configuration)
                    txHandler = StreamsTransactionEventHandler(streamHandler)
                    db.registerTransactionEventHandler(txHandler)
                } catch (e: Exception) {
                    streamsLog.error("Error initializing the streaming Connector", e)
                }
            }

            override fun stop() {
                txHandler?.let { db.unregisterTransactionEventHandler(it) }
            }
        }

    }

    interface Dependencies {
        fun graphdatabaseAPI(): GraphDatabaseAPI
        fun log(): LogService
        fun config(): Config
    }
}