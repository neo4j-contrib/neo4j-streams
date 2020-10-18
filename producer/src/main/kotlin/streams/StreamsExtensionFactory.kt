package streams

import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.kernel.availability.AvailabilityGuard
import org.neo4j.kernel.extension.ExtensionFactory
import org.neo4j.kernel.extension.ExtensionType
import org.neo4j.kernel.extension.context.ExtensionContext
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import org.neo4j.logging.internal.LogService
import streams.config.StreamsConfig
import streams.extensions.isSystemDb

class StreamsExtensionFactory : ExtensionFactory<StreamsExtensionFactory.Dependencies>(ExtensionType.DATABASE,"Streams.Producer") {
    override fun newInstance(context: ExtensionContext, dependencies: Dependencies): Lifecycle {
        val db = dependencies.graphdatabaseAPI()
        val log = dependencies.log()
        val configuration = dependencies.streamsConfig()
        val databaseManagementService = dependencies.databaseManagementService()
        val availabilityGuard = dependencies.availabilityGuard()
        return StreamsEventRouterLifecycle(availabilityGuard, db, configuration, databaseManagementService, log)
    }

    interface Dependencies {
        fun graphdatabaseAPI(): GraphDatabaseAPI
        fun log(): LogService
        fun availabilityGuard(): AvailabilityGuard
        fun databaseManagementService(): DatabaseManagementService
        fun streamsConfig(): StreamsConfig
    }
}

class StreamsEventRouterLifecycle(private val availabilityGuard: AvailabilityGuard,
                                  db: GraphDatabaseAPI,
                                  configuration: StreamsConfig,
                                  databaseManagementService: DatabaseManagementService,
                                  log: LogService): LifecycleAdapter() {

    private val streamsEventRouterAvailabilityListener: StreamsEventRouterAvailabilityListener? = if (db.isSystemDb()) {
        null
    } else {
        StreamsEventRouterAvailabilityListener(db, databaseManagementService, configuration, log)
    }

    override fun start() {
        streamsEventRouterAvailabilityListener?.also {
            availabilityGuard.addListener(it)
        }
    }

    override fun stop() {
        streamsEventRouterAvailabilityListener?.also {
            it.unavailable()
        }
    }
}
