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
import streams.events.StreamsPluginStatus
import java.lang.RuntimeException

class StreamsProcedures {

    @JvmField @Context var log: Log? = null

    @Procedure(mode = Mode.READ, name = "streams.publish")
    @Description("streams.publish(topic, config) - Allows custom streaming from Neo4j to the configured stream environment")
    fun publish(@Name("topic") topic: String?, @Name("payload") payload: Any?,
                @Name(value = "config", defaultValue = "{}") config: Map<String, Any>?) {
        checkEnabled()
        if (topic.isNullOrEmpty()) {
            log?.info("Topic empty, no message sent")
            return
        }
        if (payload == null) {
            log?.info("Payload empty, no message sent")
            return
        }
        runBlocking {
            mutex.withLock {
                val config = eventRouter!!.eventRouterConfiguration
                val streamsEvent = StreamsEventBuilder()
                        .withPayload(payload)
                        .withNodeRoutingConfiguration(config
                                .nodeRouting
                                .firstOrNull { it.topic == topic })
                        .withRelationshipRoutingConfiguration(config
                                .relRouting
                                .firstOrNull { it.topic == topic })
                        .withTopic(topic)
                        .build()
                eventRouter!!.sendEvents(topic, listOf(streamsEvent))
            }
        }
    }

    private fun checkEnabled() {
        runBlocking {
            mutex.withLock {
                if (eventRouter?.eventRouterConfiguration?.proceduresEnabled == false) {
                    throw RuntimeException("In order to use the procedure you must set streams.procedures.enabled=true")
                }
            }
        }
    }

    companion object {
        var eventRouter: StreamsEventRouter? = null

        private val mutex = Mutex()

        fun registerEventRouter(eventRouter: StreamsEventRouter?) {
            runBlocking {
                mutex.withLock {
                    StreamsProcedures.eventRouter = eventRouter
                }
            }
        }

        fun hasStatus(status: StreamsPluginStatus) = runBlocking {
            mutex.withLock {
                eventRouter != null && eventRouter?.status() == status
            }
        }

        fun isRegistered() = runBlocking {
            mutex.withLock { eventRouter != null }
        }
    }

}
