package streams

import org.neo4j.helpers.Service
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.extension.KernelExtensionFactory
import org.neo4j.kernel.impl.logging.LogService
import org.neo4j.kernel.impl.spi.KernelContext
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import streams.kafka.StreamsDataEventRouter

@Service.Implementation(KernelExtensionFactory::class)
class StreamsExtensionFactory : KernelExtensionFactory<StreamsExtensionFactory.Dependencies>("Streams") {

    override fun newInstance(context: KernelContext, dependencies: Dependencies): Lifecycle {
        val db = dependencies.graphdatabaseAPI()

        return object : LifecycleAdapter() {

            override fun start() {
                db.registerTransactionEventHandler(StreamsTransactionEventHandler(StreamsDataEventRouter()))
            }
            //FIXME implement stop()
        }

    }

    interface Dependencies {
        fun graphdatabaseAPI(): GraphDatabaseAPI
        fun log(): LogService
        fun config(): Config
    }
}