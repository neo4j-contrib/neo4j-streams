package streams

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.commons.configuration2.ImmutableConfiguration
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import org.neo4j.plugin.configuration.ConfigurationLifecycleUtils
import org.neo4j.plugin.configuration.EventType
import org.neo4j.plugin.configuration.listners.ConfigurationLifecycleListener
import streams.procedures.StreamsSinkProcedures
import streams.service.TopicUtils
import streams.utils.Neo4jUtils
import streams.utils.StreamsUtils

class StreamsSinkConfigurationListener(private val db: GraphDatabaseAPI,
                                       private val log: Log) : ConfigurationLifecycleListener {

    private val mutex = Mutex()

    var eventSink: StreamsEventSink? = null

    private val streamsTopicService = StreamsTopicService(db)

    private fun shutdown() {
        val isShuttingDown = eventSink != null
        if (isShuttingDown) {
            log.info("[Sink] Shutting down the Streams Sink Module")
        }
        eventSink?.stop()
        eventSink = null
        StreamsSinkProcedures.registerStreamsEventSink(eventSink)
        if (isShuttingDown) {
            log.info("[Sink] Shutdown of the Streams Sink Module completed")
        }
    }

    override fun onShutdown() {
        runBlocking {
            mutex.withLock {
                shutdown()
            }
        }
    }

    override fun onConfigurationChange(evt: EventType, config: ImmutableConfiguration) {
        if (config.isEmpty) {
            return
        }
        runBlocking {
            mutex.withLock {
                log.info("[Sink] An event change is detected ${evt.name}")
                shutdown()
                start(config)
            }
        }
    }

    private fun start(config: ImmutableConfiguration) {
        val configMap = ConfigurationLifecycleUtils.toMap(config)
                .mapValues { it.value.toString() }
        if (log.isDebugEnabled) {
            log.debug("[Sink] The new configuration is: $configMap")
        }
        val streamsSinkConfiguration = StreamsSinkConfiguration.from(configMap)
        streamsTopicService.clearAll()
        streamsTopicService.setAll(streamsSinkConfiguration.topics)

        val strategyMap = TopicUtils.toStrategyMap(streamsSinkConfiguration.topics,
                streamsSinkConfiguration.sourceIdStrategyConfig)
        val streamsQueryExecution = StreamsEventSinkQueryExecution(streamsTopicService, db,
                log, strategyMap)

        eventSink = StreamsEventSinkFactory
                .getStreamsEventSink(configMap,
                        streamsQueryExecution,
                        streamsTopicService,
                        log,
                        db)
        try {
            if (streamsSinkConfiguration.enabled) {
                log.info("[Sink] The Streams Sink module is starting")
                if (Neo4jUtils.isCluster(db, log)) {
                    Neo4jUtils.waitForTheLeader(db, log) { initSinkModule(streamsSinkConfiguration) }
                } else {
                    runInASingleInstance(streamsSinkConfiguration)
                }
            }
        } catch (e: Exception) {
            log.warn("Cannot start the Streams Sink module because the following exception", e)
        }

        log.info("[Sink] Registering the Streams Sink procedures")
        StreamsSinkProcedures.registerStreamsEventSink(eventSink)
    }

    private fun initSink() {
        eventSink?.start()
        eventSink?.printInvalidTopics()
    }

    private fun runInASingleInstance(streamsSinkConfiguration: StreamsSinkConfiguration) {
        // check if is writeable instance
        Neo4jUtils.executeInWriteableInstance(db) {
            if (streamsSinkConfiguration.clusterOnly) {
                log.info("""
                        |Cannot init the Streams Sink module as is forced to work only in a cluster env, 
                        |please check the value of `streams.${StreamsSinkConfigurationConstants.CLUSTER_ONLY}`
                    """.trimMargin())
            } else {
                initSinkModule(streamsSinkConfiguration)
            }
        }
    }

    private fun initSinkModule(streamsSinkConfiguration: StreamsSinkConfiguration) {
        if (streamsSinkConfiguration.checkApocTimeout > -1) {
            waitForApoc()
        } else {
            initSink()
        }
    }

    private fun waitForApoc() {
        GlobalScope.launch(Dispatchers.IO) {
            val success = StreamsUtils.blockUntilFalseOrTimeout(eventSink!!.streamsSinkConfiguration.checkApocTimeout,
                    eventSink!!.streamsSinkConfiguration.checkApocInterval) {
                val hasApoc = Neo4jUtils.hasApoc(db)
                if (!hasApoc && log.isDebugEnabled) {
                    log.debug("[Sink] APOC not loaded yet, next check in ${eventSink!!.streamsSinkConfiguration.checkApocInterval} ms")
                }
                hasApoc
            }
            if (success) {
                initSink()
            } else {
                log.info("[Sink] Streams Sink plugin not loaded as APOC are not installed")
            }
        }
    }

}