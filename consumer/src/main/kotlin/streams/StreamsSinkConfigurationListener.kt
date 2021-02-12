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
import streams.configuration.StreamsConfig
import streams.kafka.KafkaSinkConfiguration
import streams.procedures.StreamsSinkProcedures
import streams.service.TopicUtils
import streams.utils.KafkaValidationUtils
import streams.utils.Neo4jUtils
import streams.utils.StreamsUtils

class StreamsSinkConfigurationListener(private val db: GraphDatabaseAPI,
                                       private val log: Log) : ConfigurationLifecycleListener {

    private val mutex = Mutex()

    var eventSink: StreamsEventSink? = null

    private val streamsTopicService = StreamsTopicService()

    private var lastConfig: KafkaSinkConfiguration? = null

    private val producerConfig = KafkaValidationUtils.getProducerProperties()

    private fun KafkaSinkConfiguration.excludeSourceProps() = this.asProperties()
        ?.filterNot { producerConfig.contains(it.key) || it.key.toString().startsWith("streams.source") }

    // visible for testing
    fun isConfigurationChanged(configMap: Map<String, String>) = when (configMap
        .getOrDefault("streams.sink", "streams.kafka.KafkaEventSink")) {
        "streams.kafka.KafkaEventSink" ->  {
            // we validate all properties except for the ones related to the Producer
            // we use this strategy because there are some properties related to the Confluent Platform
            // that we're not able to track from the Apache Packages
            // i.e. the Schema Registry
            val kafkaConfig = KafkaSinkConfiguration.create(configMap)
            val config = kafkaConfig.excludeSourceProps()
            val lastConfig = this.lastConfig?.excludeSourceProps()
            val streamsConfig = kafkaConfig.streamsSinkConfiguration
            config != lastConfig || streamsConfig != this.lastConfig?.streamsSinkConfiguration
        }
        else -> true
    }

    override fun onShutdown() {
        runBlocking {
            mutex.withLock {
                shutdown()
            }
        }
    }

    private fun shutdown() {
        val isShuttingDown = eventSink != null
        if (isShuttingDown) {
            log.info("[Sink] Shutting down the Streams Sink Module")
        }
        eventSink?.stop()
        eventSink = null
        StreamsSinkProcedures.unregisterStreamsEventSink(db)
        if (isShuttingDown) {
            log.info("[Sink] Shutdown of the Streams Sink Module completed")
        }
    }

    override fun onConfigurationChange(evt: EventType, config: ImmutableConfiguration) {
        if (config.isEmpty) {
            return
        }
        runBlocking {
            mutex.withLock {
                log.info("[Sink] An event change is detected ${evt.name}")
                val configMap = ConfigurationLifecycleUtils.toMap(config)
                    .mapValues { it.value.toString() }
                if (!isConfigurationChanged(configMap)) {
                    log.info("[Sink] The configuration is not changed so the module will not restarted")
                    return@runBlocking
                }
                shutdown()
                if (log.isDebugEnabled) {
                    log.debug("[Sink] The new configuration is: $configMap")
                }
                start(configMap)
            }
        }
    }

    private fun start(configMap: Map<String, String>) {
        lastConfig = KafkaSinkConfiguration.create(configMap)
        val streamsSinkConfiguration = lastConfig!!.streamsSinkConfiguration
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
        StreamsSinkProcedures.registerStreamsEventSink(db, eventSink!!)
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
                        |please check the value of `${StreamsSinkConfigurationConstants.CLUSTER_ONLY}`
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