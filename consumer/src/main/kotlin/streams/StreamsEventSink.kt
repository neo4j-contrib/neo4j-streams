package streams

import org.neo4j.configuration.Config
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log

abstract class StreamsEventSink(private val config: Map<String, String>,
                                private val queryExecution: StreamsEventSinkQueryExecution,
                                private val streamsTopicService: StreamsTopicService,
                                private val log: Log,
                                private val db: GraphDatabaseAPI) {

    abstract val mappingKeys: Map<String, String>
    abstract val streamsConfigMap: Map<String, String>

    abstract fun stop()

    abstract fun start()

    abstract fun getEventConsumerFactory(): StreamsEventConsumerFactory

    open fun getEventSinkConfigMapper(): StreamsEventSinkConfigMapper = StreamsEventSinkConfigMapper(streamsConfigMap, mappingKeys)

    open fun printInvalidTopics() {}

}

object StreamsEventSinkFactory {
    fun getStreamsEventSink(config: Map<String, String>, streamsQueryExecution: StreamsEventSinkQueryExecution,
                            streamsTopicService: StreamsTopicService, log: Log, db: GraphDatabaseAPI): StreamsEventSink {
        return Class.forName(config.getOrDefault("streams.sink", "streams.kafka.KafkaEventSink"))
                .getConstructor(Map::class.java,
                        StreamsEventSinkQueryExecution::class.java,
                        StreamsTopicService::class.java,
                        Log::class.java,
                        GraphDatabaseAPI::class.java)
                .newInstance(config, streamsQueryExecution, streamsTopicService, log, db) as StreamsEventSink
    }
}

open class StreamsEventSinkConfigMapper(private val streamsConfigMap: Map<String, String>, private val mappingKeys: Map<String, String>) {
    open fun convert(config: Map<String, String>): Map<String, String> {
        val props = streamsConfigMap
                .toMutableMap()
        props += config.mapKeys { mappingKeys.getOrDefault(it.key, it.key) }
        return props
    }
}