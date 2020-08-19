package streams

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.availability.AvailabilityListener
import org.neo4j.kernel.internal.GraphDatabaseAPI
import streams.procedures.StreamsSinkProcedures
import streams.service.TopicUtils
import streams.utils.Neo4jUtils
import streams.utils.StreamsUtils
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

class StreamsEventSinkAvailabilityListener(dependencies: StreamsEventSinkExtensionFactory.Dependencies): AvailabilityListener {
    private val db = dependencies.graphdatabaseAPI()
    private val logService = dependencies.log()
    private val configuration = dependencies.config()
    private val log = logService.getUserLog(StreamsEventSinkAvailabilityListener::class.java)

    private var eventSink: StreamsEventSink? = null
    private val streamsSinkConfiguration: StreamsSinkConfiguration
    private val streamsTopicService: StreamsTopicService
    private val streamsQueryExecution: StreamsEventSinkQueryExecution

    private val mutex = Mutex()

    init {
        streamsSinkConfiguration = StreamsSinkConfiguration.from(configuration)
        streamsTopicService = StreamsTopicService(db)
        streamsTopicService.setAll(streamsSinkConfiguration.topics)
        val strategyMap = TopicUtils.toStrategyMap(streamsSinkConfiguration.topics,
                streamsSinkConfiguration.sourceIdStrategyConfig)
        streamsQueryExecution = StreamsEventSinkQueryExecution(streamsTopicService, db,
                logService.getUserLog(StreamsEventSinkQueryExecution::class.java),
                strategyMap)
    }


    override fun available() {
        runBlocking {
            mutex.withLock {
                setAvailable(db, true)
                if (eventSink == null) {
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
        try {
            log.info("Initialising the Streams Sink module")

            // start the Sink
            if (Neo4jUtils.isCluster(db, log)) {
                log.info("The Sink module is running in a cluster, checking for the ${Neo4jUtils.LEADER}")
                Neo4jUtils.waitForTheLeader(db, log) { initSinkModule() }
            } else {
                runInASingleInstance()
            }

            // Register required services for the Procedures
            StreamsSinkProcedures.registerStreamsSinkConfiguration(streamsSinkConfiguration)
            StreamsSinkProcedures.registerStreamsEventConsumerFactory(eventSink!!.getEventConsumerFactory())
            StreamsSinkProcedures.registerStreamsEventSinkConfigMapper(eventSink!!.getEventSinkConfigMapper())
            StreamsSinkProcedures.registerStreamsEventSink(eventSink!!)
        } catch (e: Exception) {
            log.error("Error initializing the streaming sink:", e)
        }
    }

    private fun runInASingleInstance() {
        // check if is writeable instance
        Neo4jUtils.executeInWriteableInstance(db) {
            if (streamsSinkConfiguration.clusterOnly) {
                log.info("""
                        |Cannot init the Streams Sink module as is forced to work only in a cluster env, 
                        |please check the value of `streams.${StreamsSinkConfigurationConstants.CLUSTER_ONLY}`
                    """.trimMargin())
            } else {
                initSinkModule()
            }
        }
    }

    private fun initSinkModule() {
        if (streamsSinkConfiguration.checkApocTimeout > -1) {
            waitForApoc()
        } else {
            initSink()
        }
    }

    private fun waitForApoc() {
        GlobalScope.launch(Dispatchers.IO) {
            val success = StreamsUtils.blockUntilTrueOrTimeout(streamsSinkConfiguration.checkApocTimeout, streamsSinkConfiguration.checkApocInterval) {
                val hasApoc = Neo4jUtils.hasApoc(db)
                if (!hasApoc && log.isDebugEnabled) {
                    log.debug("APOC not loaded yet, next check in ${streamsSinkConfiguration.checkApocInterval} ms")
                }
                hasApoc
            }
            if (success) {
                initSink()
            } else {
                log.info("Streams Sink plugin not loaded as APOC are not installed")
            }
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

        fun isAvailable(db: GraphDatabaseAPI) = available.getOrDefault(db.databaseLayout().databaseDirectory().absolutePath, false)

        fun setAvailable(db: GraphDatabaseAPI, isAvailable: Boolean): Unit = available.set(db.databaseLayout().databaseDirectory().absolutePath, isAvailable)

        fun remove(db: GraphDatabaseAPI) = available.remove(db.databaseLayout().databaseDirectory().absolutePath)
    }
}