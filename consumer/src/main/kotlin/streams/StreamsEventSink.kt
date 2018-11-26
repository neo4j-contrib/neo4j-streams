package streams

import org.neo4j.kernel.AvailabilityGuard
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.internal.GraphDatabaseAPI

abstract class StreamsEventSink(private val config: Config,
                                private val db: GraphDatabaseAPI): AvailabilityGuard.AvailabilityListener {

    abstract var streamsTopicService: StreamsTopicService?

    abstract fun stop()

    abstract fun start()

    override fun unavailable() {
        stop()
    }

    override fun available() {
        start()
    }

}

object StreamsEventSinkFactory {
    fun getStreamsEventSink(config: Config, db: GraphDatabaseAPI): StreamsEventSink {
        return Class.forName(config.raw.getOrDefault("streams.sink", "streams.kafka.KafkaEventSink"))
                .getConstructor(Config::class.java,
                        GraphDatabaseAPI::class.java)
                .newInstance(config, db) as StreamsEventSink
    }
}