package streams

import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import streams.extensions.execute
import streams.service.StreamsSinkService
import streams.service.StreamsStrategyStorage
import streams.utils.Neo4jUtils

class NotInWriteableInstanceException(message: String): RuntimeException(message)

class StreamsEventSinkQueryExecution(private val db: GraphDatabaseAPI,
                                     private val log: Log,
                                     streamsStrategyStorage: StreamsStrategyStorage):
        StreamsSinkService(streamsStrategyStorage) {

    override fun write(query: String, params: Collection<Any>) {
        if (Neo4jUtils.isWriteableInstance(db)) {
            val queryStatistics = db.execute(query, mapOf("events" to params)) { it.queryStatistics }
            if (log.isDebugEnabled) {
                log.debug("Query statistics:\n$queryStatistics")
            }
        } else {
            if (log.isDebugEnabled) {
                log.debug("Not writeable instance")
            }
            NotInWriteableInstanceException("Not writeable instance")
        }
    }
}
