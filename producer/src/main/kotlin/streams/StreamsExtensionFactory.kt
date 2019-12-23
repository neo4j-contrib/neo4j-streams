package streams

import org.neo4j.configuration.Config
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
import streams.procedures.StreamsProcedures
import streams.utils.Neo4jUtils
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
//            TODO("we should filter the data per `source.*.to.<database>`")
            if (db.databaseName() == Neo4jUtils.SYSTEM_DATABASE_NAME) {
                return
            }
            streamsLog.info("Initialising the Streams Source module")
            streamHandler = StreamsEventRouterFactory.getStreamsEventRouter(log, configuration.config)
            streamsEventRouterConfiguration = StreamsEventRouterConfiguration.from(configuration.config)
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
        }
    }

    private fun unregisterTransactionEventHandler() {
        if (streamsEventRouterConfiguration.enabled) {
            StreamsUtils.ignoreExceptions({ streamsConstraintsService.close() }, UninitializedPropertyAccessException::class.java)
            databaseManagementService.unregisterTransactionEventListener(db.databaseName(), txHandler)
        }
    }

    override fun stop() {
        unregisterTransactionEventHandler()
        StreamsUtils.ignoreExceptions({ streamHandler.stop() }, UninitializedPropertyAccessException::class.java)
    }
}
