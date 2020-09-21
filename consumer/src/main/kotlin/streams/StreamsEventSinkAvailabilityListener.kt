package streams

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.neo4j.kernel.availability.AvailabilityListener
import org.neo4j.kernel.internal.GraphDatabaseAPI
import streams.config.StreamsConfig
import streams.procedures.StreamsSinkProcedures
import streams.utils.Neo4jUtils
import streams.utils.StreamsUtils
import java.util.concurrent.ConcurrentHashMap

class StreamsEventSinkAvailabilityListener(dependencies: StreamsEventSinkExtensionFactory.Dependencies): AvailabilityListener {
    private val db = dependencies.graphdatabaseAPI()
    private val logService = dependencies.log()
    private val configuration = dependencies.streamsConfig()
    private val streamsLog = logService.getUserLog(StreamsEventSinkExtensionFactory.StreamsEventLifecycle::class.java)
    private val streamsTopicService = StreamsTopicService()
    private val log = logService.getUserLog(StreamsEventSinkFactory::class.java)
    private val dbms = dependencies.dbms()

    private var eventSink: StreamsEventSink? = null

    private val mutex = Mutex()

    override fun available() {
        try {
            configuration.loadStreamsConfiguration()
            streamsTopicService.clearAll()
            val streamsSinkConfiguration = StreamsSinkConfiguration.from(configuration, db.databaseName())
            streamsTopicService.setAll(streamsSinkConfiguration.topics)

            streamsLog.info("Initialising the Streams Sink module")
            // Create the Sink
            runBlocking {
                mutex.withLock {
                    setAvailable(db, true)
                    if (eventSink == null) {
                        val neo4jStrategyStorage = Neo4jStreamsStrategyStorage(streamsTopicService, configuration, db.databaseName())
                        val streamsQueryExecution = StreamsEventSinkQueryExecution(db, logService.getUserLog(StreamsEventSinkQueryExecution::class.java),
                                neo4jStrategyStorage)
                        // Create the Sink if not exists
                        eventSink = StreamsEventSinkFactory
                                .getStreamsEventSink(configuration,
                                        streamsQueryExecution,
                                        streamsTopicService,
                                        log,
                                        db)
                    }
                }
            }

            val whenAvailable = {
                // start the Sink
                if (Neo4jUtils.isCluster(db)) {
                    log.info("The Sink module is running in a cluster, checking for the ${StreamsUtils.LEADER}")
                    Neo4jUtils.waitForTheLeader(db, log) { initSinkModule(streamsSinkConfiguration) }
                } else {
                    // check if is writeable instance
                    runInASingleInstance(streamsSinkConfiguration)
                }
            }

            val systemDbWaitTimeout = configuration.getSystemDbWaitTimeout()
            val whenNotAvailable = {
                streamsLog.info("""
                                |Cannot start Streams Sink module because database ${StreamsUtils.SYSTEM_DATABASE_NAME} 
                                |is not available after $systemDbWaitTimeout ms
                            """.trimMargin())
            }
            // wait for the system db became available
            Neo4jUtils.executeWhenSystemDbIsAvailable(dbms,
                    configuration, whenAvailable, whenNotAvailable)

            // Register required services for the Procedures
            StreamsSinkProcedures.registerStreamsEventSink(eventSink!!)
        } catch (e: Exception) {
            streamsLog.error("Error initializing the streaming sink:", e)
        }
    }

    private fun runInASingleInstance(streamsSinkConfiguration: StreamsSinkConfiguration) {
        Neo4jUtils.executeInWriteableInstance(db) {
            if (streamsSinkConfiguration.clusterOnly) {
                log.info("""Cannot init the Streams Sink module as is forced to work only in a cluster env, 
                        |please check the value of `${StreamsConfig.CLUSTER_ONLY}`
                        """.trimMargin())
            } else {
                initSinkModule(streamsSinkConfiguration)
            }
        }
    }

    private fun initSinkModule(streamsSinkConfiguration: StreamsSinkConfiguration) {
        if (streamsSinkConfiguration.checkApocTimeout > -1) {
            GlobalScope.launch(Dispatchers.IO) {
                val success = StreamsUtils.blockUntilTrueOrTimeout(streamsSinkConfiguration.checkApocTimeout, streamsSinkConfiguration.checkApocInterval) {
                    val hasApoc = Neo4jUtils.hasApoc(db)
                    if (!hasApoc && streamsLog.isDebugEnabled) {
                        streamsLog.debug("APOC not loaded yet, next check in ${streamsSinkConfiguration.checkApocInterval} ms")
                    }
                    hasApoc
                }
                if (success) {
                    initSink()
                } else {
                    streamsLog.info("Streams Sink plugin not loaded as APOC are not installed")
                }
            }
        } else {
            initSink()
        }

    }

    private fun initSink() {
        eventSink?.start()
        eventSink?.printInvalidTopics()
    }

    override fun unavailable() = runBlocking {
        mutex.withLock {
            setAvailable(db, false)
            eventSink?.stop()
        }
        Unit
    }

    companion object {
        @JvmStatic private val available = ConcurrentHashMap<String, Boolean>()

        fun isAvailable(db: GraphDatabaseAPI) = available.getOrDefault(db.databaseName(), false)

        fun setAvailable(db: GraphDatabaseAPI, isAvailable: Boolean): Unit = available.set(db.databaseName(), isAvailable)

        fun remove(db: GraphDatabaseAPI) = available.remove(db.databaseName())
    }
}