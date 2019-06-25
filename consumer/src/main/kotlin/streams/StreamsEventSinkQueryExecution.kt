package streams

import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import streams.service.StreamsSinkService
import streams.service.TopicType
import streams.service.sink.strategy.*
import streams.utils.Neo4jUtils
import streams.utils.StreamsUtils

class StreamsEventSinkQueryExecution(private val streamsTopicService: StreamsTopicService,
                                     private val db: GraphDatabaseAPI,
                                     private val log: Log,
                                     strategyMap: Map<TopicType, Any>):
        StreamsSinkService(strategyMap) {

    override fun getTopicType(topic: String): TopicType? {
        return streamsTopicService.getTopicType(topic)
    }

    override fun getCypherTemplate(topic: String): String? {
        return "${StreamsUtils.UNWIND} ${streamsTopicService.getCypherTemplate(topic)}"
    }

    override fun write(query: String, params: Collection<Any>) {
        if (Neo4jUtils.isWriteableInstance(db)) {
            val result = db.execute(query, mapOf("events" to params))
            if (log.isDebugEnabled) {
                log.debug("Query statistics:\n${result.queryStatistics}")
            }
            result.close()
        } else {
            if (log.isDebugEnabled) {
                log.debug("Not writeable instance")
            }
        }
    }
}
