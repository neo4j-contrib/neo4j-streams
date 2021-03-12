package streams

import org.neo4j.kernel.availability.AvailabilityGuard
import org.neo4j.kernel.extension.ExtensionFactory
import org.neo4j.kernel.extension.ExtensionType
import org.neo4j.kernel.extension.context.ExtensionContext
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import org.neo4j.logging.internal.LogService
import streams.extensions.isSystemDb

class StreamsExtensionFactory : ExtensionFactory<StreamsExtensionFactory.Dependencies>(ExtensionType.DATABASE,"Streams.Producer") {
    override fun newInstance(context: ExtensionContext, dependencies: Dependencies): Lifecycle {
        val db = dependencies.graphdatabaseAPI()
        val log = dependencies.log()
        val availabilityGuard = dependencies.availabilityGuard()
        return StreamsEventRouterLifecycle(availabilityGuard, db, log)
    }

    interface Dependencies {
        fun graphdatabaseAPI(): GraphDatabaseAPI
        fun log(): LogService
        fun availabilityGuard(): AvailabilityGuard
    }
}

class StreamsEventRouterLifecycle(private val availabilityGuard: AvailabilityGuard,
                                  db: GraphDatabaseAPI,
                                  log: LogService): LifecycleAdapter() {

    private val streamsLog = log.getUserLog(StreamsEventRouterLifecycle::class.java)

    private val streamsEventRouterAvailabilityListener: StreamsEventRouterAvailabilityListener? = if (db.isSystemDb()) {
        null
    } else {
        StreamsEventRouterAvailabilityListener(db, log)
    }

    override fun start() {
        streamsEventRouterAvailabilityListener?.also {
            availabilityGuard.addListener(it)
        }
    }

    override fun stop() {
        try {
            streamsEventRouterAvailabilityListener?.also {
                it.shutdown()
                availabilityGuard.removeListener(it)
            }
        } catch (e : Throwable) {
            val message = e.message ?: "Generic error, please check the stack trace:"
            streamsLog.error(message, e)
        }
    }
}
