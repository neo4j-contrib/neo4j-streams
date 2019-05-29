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
import streams.procedures.StreamsSinkProcedures
import streams.service.TopicUtils
import streams.service.sink.strategy.SchemaIngestionStrategy
import streams.service.sink.strategy.SourceIdIngestionStrategy
import streams.utils.Neo4jUtils
import streams.utils.StreamsUtils

class StreamsEventSinkExtensionFactory : KernelExtensionFactory<StreamsEventSinkExtensionFactory.Dependencies>(ExtensionType.DATABASE,"Streams.Consumer") {

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
        private val log = dependencies.log()
        private val configuration = dependencies.config()
        private var streamsLog = log.getUserLog(StreamsEventLifecycle::class.java)

        private lateinit var eventSink: StreamsEventSink

        override fun start() {
            try {
                dependencies.availabilityGuard().addListener(object: AvailabilityListener {
                    override fun unavailable() {}

                    override fun available() {
                        val streamsSinkConfiguration = StreamsSinkConfiguration.from(configuration)

                        val streamsTopicService = StreamsTopicService(db)
                        streamsTopicService.clearAll()
                        streamsTopicService.setAll(streamsSinkConfiguration.topics)
                        val strategyMap = TopicUtils.toStrategyMap(streamsSinkConfiguration.topics,
                                streamsSinkConfiguration.sourceIdStrategyConfig)
                        val streamsQueryExecution = StreamsEventSinkQueryExecution(streamsTopicService, db,
                                log.getUserLog(StreamsEventSinkQueryExecution::class.java),
                                strategyMap)

                        // Create and start the Sink
                        eventSink = StreamsEventSinkFactory
                                .getStreamsEventSink(configuration,
                                        streamsQueryExecution,
                                        streamsTopicService,
                                        log.getUserLog(StreamsEventSinkFactory::class.java))
                        eventSink.start()
                        if (Neo4jUtils.isWriteableInstance(db)) {
                            if (streamsLog.isDebugEnabled) {
                                streamsLog.debug("Subscribed topics with Cypher queries: ${streamsTopicService.getAllCypherTemplates()}")
                                streamsLog.debug("Subscribed topics with CDC configuration: ${streamsTopicService.getAllCDCTopics()}")
                            } else {
                                streamsLog.info("Subscribed topics: ${streamsTopicService.getTopics()}")
                            }
                        }

                        // Register required services for the Procedures
                        StreamsSinkProcedures.registerStreamsSinkConfiguration(streamsSinkConfiguration)
                        eventSink = StreamsEventSinkFactory.getStreamsEventSink(configuration,
                                streamsQueryExecution,
                                streamsTopicService,
                                log.getUserLog(StreamsEventSinkFactory::class.java))
                        eventSink.start()
                        StreamsSinkProcedures.registerStreamsEventConsumerFactory(eventSink.getEventConsumerFactory())
                        StreamsSinkProcedures.registerStreamsEventSinkConfigMapper(eventSink.getEventSinkConfigMapper())
                    }

                })
            } catch (e: Exception) {
                e.printStackTrace()
                streamsLog.error("Error initializing the streaming sink", e)
            }
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

