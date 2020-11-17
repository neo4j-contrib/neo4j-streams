package streams.procedures

import kotlinx.coroutines.runBlocking
import org.neo4j.logging.Log
import org.neo4j.procedure.*
import streams.StreamsEventRouter
import streams.StreamsEventRouterConfiguration
import streams.events.StreamsEventBuilder
import java.lang.RuntimeException
import java.util.stream.Stream

class StreamsProcedures {

    @JvmField @Context var log: Log? = null

    @Procedure(mode = Mode.READ, name = "streams.publish")
    @Description("streams.publish(topic, config) - Allows custom streaming from Neo4j to the configured stream environment")
    fun publish(@Name("topic") topic: String?, @Name("payload") payload: Any?,
                @Name(value = "config", defaultValue = "{}") config: Map<String, Any>?): Stream<StreamPublishResult> {

        checkEnabled()
        if (topic.isNullOrEmpty()) {
            log?.info("Topic empty, no message sent")
            return Stream.empty();
        }
        if (payload == null) {
            log?.error("Payload empty, no message sent")
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
        if (config?.getOrDefault("synchronous", false) as Boolean) {
            val result = runBlocking {
                eventRouter.sendEventsSync(topic, listOf(streamsEvent))
            }
            return result.map { StreamPublishResult(
                    topic, payload, config,
                    it?.timestamp(),
                    it?.offset(),
                    it?.partition()?.toLong(),
                    it?.serializedKeySize()?.toLong(),
                    it?.serializedValueSize()?.toLong()
            ) }.stream()

        } else {
            eventRouter.sendEvents(topic, listOf(streamsEvent))
            return Stream.empty()
        }
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
