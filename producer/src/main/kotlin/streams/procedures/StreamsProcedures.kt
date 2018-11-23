package streams.procedures

import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Path
import org.neo4j.graphdb.Relationship
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.procedure.*
import streams.StreamsEventRouterLifecycle
import streams.events.StreamsEvent
import streams.toMap

class StreamsProcedures {

    @JvmField @Context var db: GraphDatabaseAPI? = null


    @Procedure(mode = Mode.SCHEMA, name = "streams.publish")
    @Description("streams.publish(topic, config) - Allows custom streaming from Neo4j to the configured stream environment")
    fun publish(@Name("topic") topic: String, @Name("payload") payload: Any,
                @Name(value = "config", defaultValue = "{}") config: Map<String, Any>?) {
        val newPayload = buildPayload(topic, payload)
        val streamsEvent = StreamsEvent(newPayload!!)
        getStreamsEventRouterLifecycle()?.streamHandler?.sendEvents(topic, listOf(streamsEvent))
    }

    private fun getStreamsEventRouterLifecycle() =
            db?.dependencyResolver?.resolveDependency(StreamsEventRouterLifecycle::class.java)

    fun buildPayload(topic: String, payload: Any?): Any? {
        if (payload == null) {
            return null
        }
        return when (payload) {
            is Node -> {
                val routingConfiguration = getStreamsEventRouterLifecycle()?.streamsEventRouterConfiguration?.nodeRouting
                        ?.filter { it.topic == topic}
                        ?.firstOrNull()
                if (routingConfiguration != null) {
                    routingConfiguration.filter(payload)
                } else {
                    payload.toMap()
                }
            }
            is Relationship -> {
                val routingConfiguration = getStreamsEventRouterLifecycle()?.streamsEventRouterConfiguration?.relRouting
                        ?.filter { it.topic == topic}
                        ?.firstOrNull()
                if (routingConfiguration != null) {
                    routingConfiguration.filter(payload)
                } else {
                    payload.toMap()
                }
            }
            is Path -> {
                val length = payload.length()
                val rels = payload.relationships().map { buildPayload(topic, it) }
                val nodes = payload.nodes().map { buildPayload(topic, it) }
                mapOf("length" to length, "rels" to rels, "nodes" to nodes)
            }
            is Map<*, *> -> {
                payload.mapValues { buildPayload(topic, it) }
            }
            is List<*> -> {
                payload.map { buildPayload(topic, it) }
            }
            else -> {
                payload
            }
        }
    }
}
