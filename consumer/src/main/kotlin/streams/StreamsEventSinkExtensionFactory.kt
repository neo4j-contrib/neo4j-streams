package streams

import org.neo4j.kernel.AvailabilityGuard
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.extension.KernelExtensionFactory
import org.neo4j.kernel.impl.logging.LogService
import org.neo4j.kernel.impl.spi.KernelContext
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import streams.procedures.StreamsSinkProcedures
import streams.service.TopicUtils
import streams.utils.Neo4jUtils
import streams.utils.StreamsUtils

class StreamsEventSinkExtensionFactory : KernelExtensionFactory<StreamsEventSinkExtensionFactory.Dependencies>("Streams.Consumer") {

    override fun newInstance(context: KernelContext, dependencies: Dependencies): Lifecycle {
        return StreamsEventLifecycle(dependencies)
    }

    interface Dependencies {
        fun graphdatabaseAPI(): GraphDatabaseAPI
        fun log(): LogService
        fun config(): Config
        fun availabilityGuard(): AvailabilityGuard
    }

    class StreamsEventLifecycle(private val dependencies: StreamsEventSinkExtensionFactory.Dependencies): LifecycleAdapter() {
        private val db = dependencies.graphdatabaseAPI()
        private val logService = dependencies.log()
        private val configuration = dependencies.config()
        private var streamsLog = logService.getUserLog(StreamsEventLifecycle::class.java)

        private lateinit var eventSink: StreamsEventSink

        override fun start() {
            try {
                dependencies.availabilityGuard().addListener(object: AvailabilityGuard.AvailabilityListener {
                    override fun unavailable() {}

                    override fun available() {
                        streamsLog.info("Initialising the Streams Sink module")
                        val streamsSinkConfiguration = StreamsSinkConfiguration.from(configuration)
                        val streamsTopicService = StreamsTopicService(db)
                        streamsTopicService.clearAll()
                        streamsTopicService.setAll(streamsSinkConfiguration.topics)
                        val strategyMap = TopicUtils.toStrategyMap(streamsSinkConfiguration.topics,
                                streamsSinkConfiguration.sourceIdStrategyConfig)
                        val streamsQueryExecution = StreamsEventSinkQueryExecution(streamsTopicService, db,
                                logService.getUserLog(StreamsEventSinkQueryExecution::class.java),
                                strategyMap)

                        // Create the Sink
                        val log = logService.getUserLog(StreamsEventSinkFactory::class.java)
                        eventSink = StreamsEventSinkFactory
                                .getStreamsEventSink(configuration,
                                        streamsQueryExecution,
                                        streamsTopicService,
                                        log,
                                        db)
                        // start the Sink
                        if (Neo4jUtils.isCluster(db)) {
                            log.info("The Sink module is running in a cluster, checking for the ${Neo4jUtils.LEADER}")
                            Neo4jUtils.executeInLeader(db, log) { initSinkModule() }
                        } else {
                            // check if is writeable instance
                            Neo4jUtils.executeInWriteableInstance(db) { initSinkModule() }
                        }

                        // Register required services for the Procedures
                        StreamsSinkProcedures.registerStreamsSinkConfiguration(streamsSinkConfiguration)
                        StreamsSinkProcedures.registerStreamsEventConsumerFactory(eventSink.getEventConsumerFactory())
                        StreamsSinkProcedures.registerStreamsEventSinkConfigMapper(eventSink.getEventSinkConfigMapper())
                    }

                })
            } catch (e: Exception) {
                streamsLog.error("Error initializing the streaming sink", e)
            }
        }

        private fun initSinkModule() {
            eventSink.start()
            streamsLog.info("Streams Sink module initialised")
        }

        override fun stop() {
            try {
                StreamsUtils.ignoreExceptions({ eventSink.stop() }, UninitializedPropertyAccessException::class.java)
            } catch (e : Throwable) {
                val message = e.message ?: "Generic error, please check the stack trace:"
                streamsLog.error(message, e)
            }
        }
    }
}

