package streams.procedures

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import org.neo4j.procedure.Context
import org.neo4j.procedure.Description
import org.neo4j.procedure.Mode
import org.neo4j.procedure.Name
import org.neo4j.procedure.Procedure
import streams.StreamsEventRouter
import streams.StreamsTransactionEventHandler
import streams.events.StreamsEventBuilder
import streams.events.StreamsPluginStatus
import streams.utils.StreamsUtils
import java.util.concurrent.ConcurrentHashMap
import java.util.stream.Stream

class StreamsProcedures {

    @JvmField @Context var log: Log? = null

    @JvmField @Context
    var db: GraphDatabaseService? = null

    @Procedure(mode = Mode.READ, name = "streams.publish.sync")
    @Description("streams.publish.sync(topic, payload, config) - Allows custom synchronous streaming from Neo4j to the configured stream environment")
    fun sync(@Name("topic") topic: String?, @Name("payload") payload: Any?,
             @Name(value = "config", defaultValue = "{}") config: Map<String, Any>?): Stream<StreamPublishResult> {
        checkEnabled()
        if (isTopicNullOrEmpty(topic)) {
            return Stream.empty()
        }
        checkPayloadNotNull(payload)

        val streamsEvent = buildStreamEvent(topic!!, payload!!)

        return getEventRouter(db!!)?.sendEventsSync(topic, listOf(streamsEvent), config ?: emptyMap())
                ?.map { StreamPublishResult(it) }
                .orEmpty()
                .stream()
    }

    @Procedure(mode = Mode.READ, name = "streams.publish")
    @Description("streams.publish(topic, payload, config) - Allows custom streaming from Neo4j to the configured stream environment")
    fun publish(@Name("topic") topic: String?, @Name("payload") payload: Any?,
                @Name(value = "config", defaultValue = "{}") config: Map<String, Any>?) {
        checkEnabled()
        if (isTopicNullOrEmpty(topic)) {
            return
        }
        checkPayloadNotNull(payload)

        val streamsEvent = buildStreamEvent(topic!!, payload!!)

        getEventRouter(db!!)?.sendEvents(topic, listOf(streamsEvent), config ?: emptyMap())
    }

    private fun isTopicNullOrEmpty(topic: String?): Boolean {
        return if (topic.isNullOrEmpty()) {
            log?.info("Topic empty, no message sent")
            true
        } else {
            false
        }
    }

    private fun checkEnabled() {
        if (getEventRouter(db!!)?.eventRouterConfiguration?.proceduresEnabled == false) {
            throw RuntimeException("In order to use the procedure you must set streams.procedures.enabled=true")
        }
    }

    private fun checkPayloadNotNull(payload: Any?) {
        if (payload == null) {
            log?.error("Payload empty, no message sent")
            throw RuntimeException("Payload may not be null")
        }
    }

    private fun buildStreamEvent(topic: String, payload: Any) = StreamsEventBuilder()
            .withPayload(payload)
            .withNodeRoutingConfiguration(getEventRouter(db!!)?.eventRouterConfiguration
                    ?.nodeRouting
                    ?.filter { it.topic == topic }
                    ?.firstOrNull())
            .withRelationshipRoutingConfiguration(getEventRouter(db!!)?.eventRouterConfiguration
                    ?.relRouting
                    ?.filter { it.topic == topic }
                    ?.firstOrNull())
            .withTopic(topic)
            .build()

    companion object {
        private val cache = ConcurrentHashMap<String, Pair<StreamsEventRouter, StreamsTransactionEventHandler>>()

        private fun getEventRouter(db: GraphDatabaseService) = cache[StreamsUtils.getName(db as GraphDatabaseAPI)]?.first

        fun registerEventRouter(db: GraphDatabaseAPI, pair: Pair<StreamsEventRouter, StreamsTransactionEventHandler>) = cache
            .put(StreamsUtils.getName(db), pair)

        fun unregisterEventRouter(db: GraphDatabaseAPI) = cache.remove(StreamsUtils.getName(db))

        fun hasStatus(db: GraphDatabaseAPI, status: StreamsPluginStatus) = cache[StreamsUtils.getName(db)]?.second?.status() == status

        fun isRegistered(db: GraphDatabaseAPI) = getEventRouter(db) != null
    }

}
