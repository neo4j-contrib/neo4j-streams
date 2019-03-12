package streams.procedures

import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.producer.RecordMetadata
import org.neo4j.logging.Log
import org.neo4j.procedure.*
import streams.StreamsEventRouter
import streams.StreamsEventRouterConfiguration
import streams.events.StreamsEventBuilder
import java.lang.RuntimeException
import java.util.stream.Stream

class PublishResult {
    @JvmField public var topic: String
    @JvmField public var payload: Any
    @JvmField public var config: Map<String,Any>?
    @JvmField public var offset: Long
    @JvmField public var partition: Long
    @JvmField public var keySize: Long
    @JvmField public var valueSize: Long
    @JvmField public var timestamp: Long

    constructor(topic: String, payload: Any, config: Map<String,Any>?, md: RecordMetadata) {
        this.topic = topic;
        this.payload = payload;
        this.config = config;

        this.offset = md.offset()
        this.partition = md.partition().toLong()
        this.keySize = md.serializedKeySize().toLong()
        this.valueSize = md.serializedValueSize().toLong()
        this.timestamp = md.timestamp()
    }
}

class StreamsProcedures {

    @JvmField @Context var log: Log? = null

    @Procedure(mode = Mode.SCHEMA, name = "streams.publish")
    @Description("streams.publish(topic, config) - Allows custom streaming from Neo4j to the configured stream environment")
    fun publish(@Name("topic") topic: String?, @Name("payload") payload: Any?,
                @Name(value = "config", defaultValue = "{}") config: Map<String, Any>?): Stream<PublishResult> {
        checkEnabled()
        if (topic.isNullOrEmpty()) {
            log?.info("Topic empty, no message sent")
            return Stream.empty();
        }
        if (payload == null) {
            log?.info("Payload empty, no message sent")
            throw RuntimeException("Payload may not be null")
        }
        val streamsEvent = StreamsEventBuilder()
                .withPayload(payload)
                .withNodeRoutingConfiguration(StreamsProcedures.eventRouterConfiguration
                        .nodeRouting
                        .filter { it.topic == topic }
                        .firstOrNull())
                .withRelationshipRoutingConfiguration(StreamsProcedures.eventRouterConfiguration
                        .relRouting
                        .filter { it.topic == topic }
                        .firstOrNull())
                .withTopic(topic)
                .build()
        val result = StreamsProcedures.eventRouter.sendEventsSync(topic, listOf(streamsEvent))

        return result.map { PublishResult(topic, payload, config, it) }.stream()
    }

    private fun checkEnabled() {
        if (!StreamsProcedures.eventRouterConfiguration.proceduresEnabled) {
            throw RuntimeException("In order to use the procedure you must set streams.procedures.enabled=true")
        }
    }

    companion object {
        private lateinit var eventRouter: StreamsEventRouter
        private lateinit var eventRouterConfiguration: StreamsEventRouterConfiguration

        fun registerEventRouter(eventRouter: StreamsEventRouter) {
            this.eventRouter = eventRouter
        }

        fun registerEventRouterConfiguration(eventRouterConfiguration: StreamsEventRouterConfiguration) {
            this.eventRouterConfiguration = eventRouterConfiguration
        }
    }

}
