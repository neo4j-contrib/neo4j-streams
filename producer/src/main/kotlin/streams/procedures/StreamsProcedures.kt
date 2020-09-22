package streams.procedures

import kotlinx.coroutines.runBlocking
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.logging.Log
import org.neo4j.procedure.Context
import org.neo4j.procedure.Description
import org.neo4j.procedure.Mode
import org.neo4j.procedure.Name
import org.neo4j.procedure.Procedure
import streams.StreamsEventRouter
import streams.StreamsEventRouterConfiguration
import streams.events.StreamsEventBuilder
import java.util.concurrent.ConcurrentHashMap

data class StreamsEventSinkStoreEntry(val eventRouter: StreamsEventRouter,
                                      val eventRouterConfiguration: StreamsEventRouterConfiguration)
class StreamsProcedures {

    @JvmField @Context
    var db: GraphDatabaseService? = null

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
                .withNodeRoutingConfiguration(getStreamsEventSinkStoreEntry()
                        .eventRouterConfiguration
                        .nodeRouting
                        .firstOrNull { it.topic == topic })
                .withRelationshipRoutingConfiguration(getStreamsEventSinkStoreEntry()
                        .eventRouterConfiguration
                        .relRouting
                        .firstOrNull { it.topic == topic })
                .withTopic(topic)
                .build()
        getStreamsEventSinkStoreEntry().eventRouter.sendEvents(topic, listOf(streamsEvent))
    }

    private fun checkEnabled() {
        if (!getStreamsEventSinkStoreEntry().eventRouterConfiguration.proceduresEnabled) {
            throw RuntimeException("In order to use the procedure you must set streams.procedures.enabled=true")
        }
    }

    private fun getStreamsEventSinkStoreEntry() = streamsEventRouterStore[db!!.databaseName()]!!

    companion object {

        private val streamsEventRouterStore = ConcurrentHashMap<String, StreamsEventSinkStoreEntry>()

        fun register(databaseName: String,
                     evtRouter: StreamsEventRouter,
                     evtConf: StreamsEventRouterConfiguration) = runBlocking {
            streamsEventRouterStore[databaseName] = StreamsEventSinkStoreEntry(evtRouter, evtConf)
        }
    }

}
