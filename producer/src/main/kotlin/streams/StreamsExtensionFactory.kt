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

class StreamsEventRouterLifecycle(val db: GraphDatabaseAPI, val configuration: Config,
                                  private val availabilityGuard: AvailabilityGuard,
                                  private val log: LogService): LifecycleAdapter() {
    private val streamsLog = log.getUserLog(StreamsEventRouterLifecycle::class.java)
    private lateinit var txHandler: StreamsTransactionEventHandler
    private lateinit var streamsConstraintsService: StreamsConstraintsService
    private lateinit var streamHandler: StreamsEventRouter
    private lateinit var streamsEventRouterConfiguration: StreamsEventRouterConfiguration

    override fun start() {
        try {
            streamsLog.info("Initialising the Streams Source module")
            streamHandler = StreamsEventRouterFactory.getStreamsEventRouter(log, configuration)
            streamsEventRouterConfiguration = StreamsEventRouterConfiguration.from(configuration.raw)
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
            db.registerTransactionEventHandler(txHandler)
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
            StreamsUtils.ignoreExceptions({ db.unregisterTransactionEventHandler(txHandler) }, UninitializedPropertyAccessException::class.java)
        }
    }

    override fun stop() {
        unregisterTransactionEventHandler()
        StreamsUtils.ignoreExceptions({ streamHandler.stop() }, UninitializedPropertyAccessException::class.java)
    }
}
