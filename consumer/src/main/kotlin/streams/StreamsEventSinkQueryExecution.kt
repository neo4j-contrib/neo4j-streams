package streams

import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log

class StreamsEventSinkQueryExecution(private val streamsTopicService: StreamsTopicService, private val db: GraphDatabaseAPI) {

    private val UNWIND: String = "UNWIND {events} AS event"

    fun execute(topic: String, params: Collection<Any>) {
        val cypherQuery = streamsTopicService.get(topic)
        if (cypherQuery == null) {
            return
        }
        db.execute("$UNWIND $cypherQuery", mapOf("events" to params)).close()
    }

    fun execute(map: Map<String, Collection<Any>>) {
        map.entries.forEach{ execute(it.key, it.value) }
    }

}

