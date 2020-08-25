package streams

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.neo4j.kernel.availability.AvailabilityListener
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.internal.LogService
import streams.procedures.StreamsProcedures
import streams.utils.StreamsUtils

class StreamsEventRouterAvailabilityListener(private val db: GraphDatabaseAPI,
                                             configuration: Config,
                                             log: LogService): AvailabilityListener {
    private val streamsLog = log.getUserLog(StreamsEventRouterAvailabilityListener::class.java)
    private val txHandler: StreamsTransactionEventHandler
    private val streamsConstraintsService: StreamsConstraintsService
    private val streamHandler: StreamsEventRouter
    private val streamsEventRouterConfiguration: StreamsEventRouterConfiguration
    private var registered = false

    private val mutex = Mutex()

    init {
        streamsLog.info("Initialising the Streams Source module")
        streamHandler = StreamsEventRouterFactory.getStreamsEventRouter(log, configuration)
        streamsEventRouterConfiguration = StreamsEventRouterConfiguration.from(configuration.raw)
        StreamsProcedures.registerEventRouter(eventRouter = streamHandler)
        StreamsProcedures.registerEventRouterConfiguration(eventRouterConfiguration = streamsEventRouterConfiguration)
        streamsConstraintsService = StreamsConstraintsService(db, streamsEventRouterConfiguration.schemaPollingInterval)
        txHandler = StreamsTransactionEventHandler(streamHandler, streamsConstraintsService, streamsEventRouterConfiguration)
        streamsLog.info("Streams Source module initialised")
    }

    private fun registerTransactionEventHandler() = runBlocking {
        mutex.withLock {
            if (streamsEventRouterConfiguration.enabled && !registered) {
                streamHandler.start()
                streamHandler.printInvalidTopics()
                db.registerTransactionEventHandler(txHandler)
                streamsConstraintsService.start()
                registered = true
            }
        }
    }

    private fun unregisterTransactionEventHandler() = runBlocking {
        mutex.withLock {
            if (streamsEventRouterConfiguration.enabled && registered) {
                StreamsUtils.ignoreExceptions({ streamsConstraintsService.close() },
                        UninitializedPropertyAccessException::class.java)
                StreamsUtils.ignoreExceptions({ db.unregisterTransactionEventHandler(txHandler) },
                        UninitializedPropertyAccessException::class.java)
                registered = false
            }
        }
    }
    override fun available() {
        registerTransactionEventHandler()
    }

    override fun unavailable() {
        unregisterTransactionEventHandler()
    }

}