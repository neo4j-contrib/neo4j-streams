package streams

import org.neo4j.kernel.availability.AvailabilityGuard
import org.neo4j.kernel.availability.AvailabilityListener
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.extension.ExtensionType
import org.neo4j.kernel.extension.KernelExtensionFactory
import org.neo4j.kernel.impl.spi.KernelContext
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import org.neo4j.logging.internal.LogService
import streams.procedures.StreamsProcedures
import streams.utils.StreamsUtils

class StreamsExtensionFactory : KernelExtensionFactory<StreamsExtensionFactory.Dependencies>(ExtensionType.DATABASE,"Streams.Producer") {
    override fun newInstance(context: KernelContext, dependencies: Dependencies): Lifecycle {
        val db = dependencies.graphdatabaseAPI()
        val log = dependencies.log()
        val configuration = dependencies.config()
        return StreamsEventRouterLifecycle(db, configuration, dependencies.availabilityGuard(), log)
    }

    interface Dependencies {
        fun graphdatabaseAPI(): GraphDatabaseAPI
        fun log(): LogService
        fun config(): Config
        fun availabilityGuard(): AvailabilityGuard
    }
}

class StreamsEventRouterLifecycle(db: GraphDatabaseAPI,
                                  configuration: Config,
                                  private val availabilityGuard: AvailabilityGuard,
                                  log: LogService): LifecycleAdapter() {
    private val streamsEventRouterAvailabilityListener = StreamsEventRouterAvailabilityListener(db, log)

    override fun start() {
        availabilityGuard.addListener(streamsEventRouterAvailabilityListener)
    }

    override fun stop() {
        streamsEventRouterAvailabilityListener.shutdown()
    }
}
