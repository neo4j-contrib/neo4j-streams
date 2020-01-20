package streams

import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.kernel.availability.AvailabilityGuard
import org.neo4j.kernel.availability.AvailabilityListener
import org.neo4j.kernel.extension.ExtensionFactory
import org.neo4j.kernel.extension.ExtensionType
import org.neo4j.kernel.extension.context.ExtensionContext
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import org.neo4j.logging.internal.LogService
import streams.config.StreamsConfig
import streams.extensions.isSystemDb
import streams.procedures.StreamsProcedures
import streams.utils.StreamsUtils

class StreamsExtensionFactory : ExtensionFactory<StreamsExtensionFactory.Dependencies>(ExtensionType.DATABASE,"Streams.Producer") {
    override fun newInstance(context: ExtensionContext, dependencies: Dependencies): Lifecycle {
        val db = dependencies.graphdatabaseAPI()
        val log = dependencies.log()
        val configuration = dependencies.streamsConfig()
        val databaseManagementService = dependencies.databaseManagementService()
        val availabilityGuard = dependencies.availabilityGuard()
        return StreamsEventRouterLifecycle(db, configuration, databaseManagementService, availabilityGuard, log)
    }

    interface Dependencies {
        fun graphdatabaseAPI(): GraphDatabaseAPI
        fun log(): LogService
        fun availabilityGuard(): AvailabilityGuard
        fun databaseManagementService(): DatabaseManagementService
        fun streamsConfig(): StreamsConfig
    }
}

class StreamsEventRouterLifecycle(private val db: GraphDatabaseAPI,
                                  private val configuration: StreamsConfig,
                                  private val databaseManagementService: DatabaseManagementService,
                                  private val availabilityGuard: AvailabilityGuard,
                                  private val log: LogService): LifecycleAdapter() {
    private val streamsLog = log.getUserLog(StreamsEventRouterLifecycle::class.java)
    private lateinit var txHandler: StreamsTransactionEventHandler
    private lateinit var streamsConstraintsService: StreamsConstraintsService
    private lateinit var streamHandler: StreamsEventRouter
    private lateinit var streamsEventRouterConfiguration: StreamsEventRouterConfiguration

    override fun start() {
        try {
            if (db.isSystemDb()) {
                return
            }
            configuration.loadStreamsConfiguration()
            streamsLog.info("Initialising the Streams Source module")
            streamHandler = StreamsEventRouterFactory.getStreamsEventRouter(log, configuration, db.databaseName())
            streamsEventRouterConfiguration = StreamsEventRouterConfiguration.from(configuration, db.databaseName())
            StreamsProcedures.registerEventRouter(eventRouter = streamHandler)
            StreamsProcedures.registerEventRouterConfiguration(eventRouterConfiguration = streamsEventRouterConfiguration)
            streamHandler.start()
            streamHandler.printInvalidTopics()
            registerTransactionEventHandler()
            streamsLog.info("Streams Source module initialised")
        } catch (e: Exception) {
            streamsLog.error("Error initializing the streaming producer:", e)
        }
    }

    private fun registerTransactionEventHandler() {
        if (streamsEventRouterConfiguration.enabled) {
            streamsConstraintsService = StreamsConstraintsService(db, streamsEventRouterConfiguration.schemaPollingInterval)
            txHandler = StreamsTransactionEventHandler(streamHandler, streamsConstraintsService, streamsEventRouterConfiguration)
            databaseManagementService.registerTransactionEventListener(db.databaseName(), txHandler)
            availabilityGuard.addListener(object: AvailabilityListener {
                override fun unavailable() {}

                override fun available() {
                    streamsConstraintsService.start()
                }
            })
            if (streamsLog.isDebugEnabled) {
                streamsLog.info("Streams Source transaction handler initialised with the following configuration: $streamsEventRouterConfiguration")
            } else {
                streamsLog.info("Streams Source transaction handler initialised")
            }
        }
    }

    private fun unregisterTransactionEventHandler() {
        if (streamsEventRouterConfiguration.enabled) {
            StreamsUtils.ignoreExceptions({ streamsConstraintsService.close() }, UninitializedPropertyAccessException::class.java)
            StreamsUtils.ignoreExceptions({ databaseManagementService.unregisterTransactionEventListener(db.databaseName(), txHandler) }, UninitializedPropertyAccessException::class.java)
        }
    }

    override fun stop() {
        unregisterTransactionEventHandler()
        StreamsUtils.ignoreExceptions({ streamHandler.stop() }, UninitializedPropertyAccessException::class.java)
    }
}
