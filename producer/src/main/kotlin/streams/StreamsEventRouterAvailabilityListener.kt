package streams

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.kernel.availability.AvailabilityListener
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.internal.LogService
import streams.config.StreamsConfig
import streams.procedures.StreamsProcedures
import streams.utils.StreamsUtils
import java.lang.IllegalStateException

class StreamsEventRouterAvailabilityListener(private val db: GraphDatabaseAPI,
                                             private val databaseManagementService: DatabaseManagementService,
                                             private val configuration: StreamsConfig,
                                             private val log: LogService): AvailabilityListener {
    private val streamsLog = log.getUserLog(StreamsEventRouterAvailabilityListener::class.java)
    private var streamsConstraintsService: StreamsConstraintsService? = null
    private var txHandler: StreamsTransactionEventHandler? = null
    private var streamHandler: StreamsEventRouter? = null

    private val mutex = Mutex()


    private fun registerTransactionEventHandler() = runBlocking {
        mutex.withLock {
            configuration.loadStreamsConfiguration()
            streamsLog.info("Initialising the Streams Source module")
            val streamsEventRouterConfiguration = StreamsEventRouterConfiguration.from(configuration, db.databaseName())
            if (streamsConstraintsService == null) {
                streamsConstraintsService = StreamsConstraintsService(db, streamsEventRouterConfiguration.schemaPollingInterval)
            }
            if (streamsEventRouterConfiguration.enabled) {
                streamHandler = StreamsEventRouterFactory.getStreamsEventRouter(log, configuration, db.databaseName())
                txHandler = StreamsTransactionEventHandler(streamHandler!!, streamsConstraintsService!!, streamsEventRouterConfiguration)
                streamHandler!!.start()
                streamHandler!!.printInvalidTopics()
                streamsConstraintsService!!.start()
                databaseManagementService.registerTransactionEventListener(db.databaseName(), txHandler)
                StreamsProcedures.register(db.databaseName(), streamHandler!!, streamsEventRouterConfiguration)
                if (streamsLog.isDebugEnabled) {
                    streamsLog.info("Streams Source transaction handler initialised with the following configuration: $streamsEventRouterConfiguration")
                } else {
                    streamsLog.info("Streams Source transaction handler initialised")
                }
            }
        }
    }

    private fun unregisterTransactionEventHandler() = runBlocking {
        mutex.withLock {
            StreamsUtils.ignoreExceptions({
                val streamsEventRouterConfiguration = StreamsEventRouterConfiguration
                        .from(configuration, db.databaseName())
                if (streamsEventRouterConfiguration.enabled) {
                    streamHandler?.stop()
                    streamsConstraintsService?.close()
                    databaseManagementService.unregisterTransactionEventListener(db.databaseName(), txHandler)
                }
            }, UninitializedPropertyAccessException::class.java, IllegalStateException::class.java)
        }
    }

    override fun available() {
        val whenAvailable = {
            registerTransactionEventHandler()
        }
        val systemDbWaitTimeout = configuration.getSystemDbWaitTimeout()
        val whenNotAvailable = {
            streamsLog.info("""
                                |Cannot start Streams Source module because database ${StreamsUtils.SYSTEM_DATABASE_NAME} 
                                |is not available after $systemDbWaitTimeout ms
                            """.trimMargin())
        }
        // wait for the system db became available
        StreamsUtils.executeWhenSystemDbIsAvailable(databaseManagementService,
                configuration, whenAvailable, whenNotAvailable)

    }

    override fun unavailable() {
        unregisterTransactionEventHandler()
    }
}