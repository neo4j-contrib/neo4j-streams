package streams.procedures

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.commons.lang3.StringUtils
import org.neo4j.logging.Log
import org.neo4j.procedure.*
import streams.StreamsEventRouter
import streams.StreamsEventRouterConfiguration
import streams.events.StreamsEventBuilder
import java.lang.RuntimeException

class StreamsProcedures {

    @JvmField @Context var log: Log? = null

    @Procedure(mode = Mode.READ, name = "streams.publish")
    @Description("streams.publish(topic, config) - Allows custom streaming from Neo4j to the configured stream environment")
    fun publish(@Name("topic") topic: String?, @Name("payload") payload: Any?,
                @Name(value = "config", defaultValue = "{}") config: Map<String, Any>?) = runBlocking {
        checkEnabled()
        if (topic.isNullOrEmpty()) {
            log?.info("Topic empty, no message sent")
            return@runBlocking
        }
        if (payload == null) {
            log?.info("Payload empty, no message sent")
            return@runBlocking
        }
        val streamsEvent = StreamsEventBuilder()
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
        mutex.withLock { eventRouter.sendEvents(topic, listOf(streamsEvent)) }
    }

    private fun checkEnabled() {
        if (!eventRouterConfiguration.proceduresEnabled) {
            throw RuntimeException("In order to use the procedure you must set streams.procedures.enabled=true")
        }
    }

    companion object {

        private val mutex = Mutex()
        private lateinit var eventRouter: StreamsEventRouter
        private lateinit var eventRouterConfiguration: StreamsEventRouterConfiguration

        fun registerEventRouter(evtRouter: StreamsEventRouter) = runBlocking {
            mutex.withLock { eventRouter = evtRouter }
        }

        fun registerEventRouterConfiguration(evtConf: StreamsEventRouterConfiguration) = runBlocking {
            mutex.withLock { eventRouterConfiguration = evtConf }
        }
    }

}
