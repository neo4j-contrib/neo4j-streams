package streams

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.commons.configuration2.ImmutableConfiguration
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import org.neo4j.plugin.configuration.ConfigurationLifecycleUtils
import org.neo4j.plugin.configuration.EventType
import org.neo4j.plugin.configuration.listners.ConfigurationLifecycleListener
import streams.procedures.StreamsProcedures

class StreamsRouterConfigurationListener(private val db: GraphDatabaseAPI,
                                         private val log: Log) : ConfigurationLifecycleListener {
    private val mutex = Mutex()

    private var txHandler: StreamsTransactionEventHandler? = null
    private var streamsConstraintsService: StreamsConstraintsService? = null
    private var streamHandler: StreamsEventRouter? = null
    private var streamsEventRouterConfiguration: StreamsEventRouterConfiguration? = null

    override fun onShutdown() {
        runBlocking {
            mutex.withLock {
                shutdown()
            }
        }
    }

    private fun shutdown() {
        if (streamsEventRouterConfiguration?.enabled == true) {
            streamsConstraintsService?.close()
            streamHandler?.stop()
            streamHandler = null
            StreamsProcedures.registerEventRouter(streamHandler)
            if (txHandler != null) {
                db.unregisterTransactionEventHandler(txHandler)
                txHandler = null
            }
        }
    }

    private fun start(config: ImmutableConfiguration) {
        log.info("[Source] Initialising the Streams Source module")
        val configMap = ConfigurationLifecycleUtils.toMap(config)
                .mapValues { it.value.toString() }
        if (log.isDebugEnabled) {
            log.debug("[Source] The new configuration is: $configMap")
        }
        streamsEventRouterConfiguration = StreamsEventRouterConfiguration.from(configMap)
        streamHandler = StreamsEventRouterFactory.getStreamsEventRouter(configMap, log)
        if (streamsConstraintsService == null) {
            streamsConstraintsService = StreamsConstraintsService(db, streamsEventRouterConfiguration!!.schemaPollingInterval)
        }
        txHandler = StreamsTransactionEventHandler(streamHandler!!, streamsConstraintsService!!)
        if (streamsEventRouterConfiguration?.enabled == true) {
            streamsConstraintsService!!.start()
            streamHandler!!.start()
            streamHandler!!.printInvalidTopics()
            db.registerTransactionEventHandler(txHandler)
        }
        StreamsProcedures.registerEventRouter(streamHandler)
        log.info("[Source] Streams Source module initialised")
    }

    override fun onConfigurationChange(evt: EventType, config: ImmutableConfiguration) {
        if (config.isEmpty) {
            return
        }
        runBlocking {
            mutex.withLock {
                log.info("[Source] An event change is detected ${evt.name}")
                shutdown()
                start(config)
            }
        }
    }
}