package streams.procedures

import org.apache.commons.lang3.StringUtils
import org.neo4j.logging.Log
import org.neo4j.procedure.*
import streams.StreamsEventRouter
import streams.StreamsEventRouterConfiguration
import streams.events.StreamsEventBuilder

class StreamsProcedures {

    @JvmField @Context var log: Log? = null

    @Procedure(mode = Mode.SCHEMA, name = "streams.publish")
    @Description("streams.publish(topic, config) - Allows custom streaming from Neo4j to the configured stream environment")
    fun publish(@Name("topic") topic: String?, @Name("payload") payload: Any?,
                @Name(value = "config", defaultValue = "{}") config: Map<String, Any>?) {

        if (topic == null || topic == StringUtils.EMPTY) {
            log?.info("Topic empty, no message sent")
            return
        }
        if (payload == null) {
            log?.info("Payload empty, no message sent")
            return
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
        StreamsProcedures.eventRouter.sendEvents(topic, listOf(streamsEvent))
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
