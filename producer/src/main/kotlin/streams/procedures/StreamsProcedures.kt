package streams.procedures

import org.neo4j.logging.Log
import org.neo4j.procedure.*
import streams.StreamsEventRouter
import streams.StreamsEventRouterConfiguration
import streams.events.StreamsEventBuilder
import java.lang.RuntimeException
import java.util.stream.Stream

class StreamsProcedures {

    @JvmField @Context var log: Log? = null

    @Procedure(mode = Mode.READ, name = "streams.publish.sync")
    @Description("streams.publish.sync(topic, payload, config) - Allows custom synchronous streaming from Neo4j to the configured stream environment")
    fun sync(@Name("topic") topic: String?, @Name("payload") payload: Any?,
             @Name(value = "config", defaultValue = "{}") config: Map<String, Any>?): Stream<StreamPublishResult> {

        checkEnabled()
        if (topic.isNullOrEmpty()) {
            log?.info("Topic empty, no message sent")
            return Stream.empty();
        }
        checkPayloadNull(payload)

        val streamsEvent = buildStreamEvent(topic, payload!!)

        return eventRouter.sendEventsSync(topic, listOf(streamsEvent)).map { StreamPublishResult(
                topic,
                payload,
                config,
                it?.timestamp(),
                it?.offset(),
                it?.partition()?.toLong(),
                it?.serializedKeySize()?.toLong(),
                it?.serializedValueSize()?.toLong()
        ) }.stream()
    }

    @Procedure(mode = Mode.READ, name = "streams.publish")
    @Description("streams.publish(topic, payload, config) - Allows custom streaming from Neo4j to the configured stream environment")
    fun publish(@Name("topic") topic: String?, @Name("payload") payload: Any?,
                @Name(value = "config", defaultValue = "{}") config: Map<String, Any>?) {

        checkEnabled()
        if (topic.isNullOrEmpty()) {
            log?.info("Topic empty, no message sent")
            return
        }
        checkPayloadNull(payload)

        val streamsEvent = buildStreamEvent(topic, payload!!)

        eventRouter.sendEvents(topic, listOf(streamsEvent))
    }

    private fun checkEnabled() {
        if (!eventRouterConfiguration.proceduresEnabled) {
            throw RuntimeException("In order to use the procedure you must set streams.procedures.enabled=true")
        }
    }

    private fun checkPayloadNull(payload: Any?) {
        if (payload == null) {
            log?.error("Payload empty, no message sent")
            throw RuntimeException("Payload may not be null")
        }
    }

    private fun buildStreamEvent(topic: String, payload: Any) = StreamsEventBuilder()
            .withPayload(payload)
            .withNodeRoutingConfiguration(eventRouterConfiguration
                    .nodeRouting
                    .filter { it.topic == topic }
                    .firstOrNull())
            .withRelationshipRoutingConfiguration(eventRouterConfiguration
                    .relRouting
                    .filter { it.topic == topic }
                    .firstOrNull())
            .withTopic(topic)
            .build()

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
