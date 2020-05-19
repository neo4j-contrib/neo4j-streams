package streams

import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.serialization.BytesDeserializer
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import streams.service.StreamsSinkEntity
import streams.service.StreamsSinkService
import streams.service.TopicType
import streams.utils.Neo4jUtils
import streams.utils.StreamsUtils

class StreamsEventSinkQueryExecution(private val streamsTopicService: StreamsTopicService,
                                     private val db: GraphDatabaseAPI,
                                     private val log: Log,
                                     strategyMap: Map<TopicType, Any>,
                                     private val eventPrefixTimestamp: String,
                                     private val eventPrefixHeaders: String,
                                     private val eventPrefixKey: String) :
        StreamsSinkService(strategyMap) {

    override fun getTopicType(topic: String): TopicType? {
        return streamsTopicService.getTopicType(topic)
    }

    override fun getCypherTemplate(topic: String): String? {
        return "${StreamsUtils.UNWIND} ${streamsTopicService.getCypherTemplate(topic)}"
    }

    override fun write(query: String, records: Collection<Any>) {
        if (Neo4jUtils.isWriteableInstance(db)) {
            val events = records.map {
                if (it is StreamsSinkEntity) {
                    val record = mutableMapOf<String, Any?>()
                    val key = it.key ?: mapOf<Any, Any>()
                    val value = it.value ?: mapOf<Any, Any>()

                    record[eventPrefixTimestamp] = it.timestamp
                    record.putAll(mapOf(eventPrefixHeaders to it.headers))
                    record.putAll(mapOf(eventPrefixKey to key))
                    record.putAll(value as Map<String, Any>)

                    record
                } else {
                    it
                }
            }

            val result = db.execute(query, mapOf("events" to events))
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
