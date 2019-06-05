package streams

import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log

abstract class StreamsEventSink(private val config: Config,
                                private val queryExecution: StreamsEventSinkQueryExecution,
                                private val streamsTopicService: StreamsTopicService,
                                private val log: Log,
                                private val db: GraphDatabaseAPI) {

    abstract fun stop()

    abstract fun start()

    abstract fun getEventConsumerFactory(): StreamsEventConsumerFactory

    abstract fun getEventSinkConfigMapper(): StreamsEventSinkConfigMapper

}

abstract class StreamsEventConsumer(private val log: Log) {

    abstract fun stop()

    abstract fun withTopics(topics: Set<String>): StreamsEventConsumer

    abstract fun start()

    abstract fun read(topicConfig: Map<String, Any> = emptyMap(), action: (String, List<Any>) -> Unit)

}

abstract class StreamsEventConsumerFactory {
    abstract fun createStreamsEventConsumer(config: Map<String, String>, log: Log): StreamsEventConsumer
}

object StreamsEventSinkFactory {
    fun getStreamsEventSink(config: Config, streamsQueryExecution: StreamsEventSinkQueryExecution,
                            streamsTopicService: StreamsTopicService, log: Log, db: GraphDatabaseAPI): StreamsEventSink {
        return Class.forName(config.raw.getOrDefault("streams.sink", "streams.kafka.KafkaEventSink"))
                .getConstructor(Config::class.java,
                        StreamsEventSinkQueryExecution::class.java,
                        StreamsTopicService::class.java,
                        Log::class.java,
                        GraphDatabaseAPI::class.java)
                .newInstance(config, streamsQueryExecution, streamsTopicService, log, db) as StreamsEventSink
    }
}

abstract class StreamsEventSinkConfigMapper(private val baseConfiguration: Map<String, String>, private val mapping: Map<String, String>) {
    abstract fun convert(config: Map<String, String>): Map<String, String>
}