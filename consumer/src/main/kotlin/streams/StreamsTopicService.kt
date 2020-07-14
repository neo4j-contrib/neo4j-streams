package streams

import org.neo4j.kernel.internal.GraphDatabaseAPI
import streams.service.TopicType
import streams.service.Topics
import streams.utils.Neo4jUtils
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

class StreamsTopicService(db: GraphDatabaseAPI) {
    private val log = Neo4jUtils.getLogService(db).getUserLog(StreamsTopicService::class.java)

    private val storage = ConcurrentHashMap<TopicType, Any>()

    fun clearAll() {
        storage.clear()
    }

    fun set(topicType: TopicType, data: Any) {
        val runtimeException = RuntimeException("Unsupported data $data for topic type $topicType")
        var oldData = storage[topicType]
        oldData = oldData ?: when (data) {
            is Map<*, *> -> emptyMap<String, Any?>()
            is Collection<*> -> emptyList<String>()
            else -> throw runtimeException
        }
        val newData = when (oldData) {
            is Map<*, *> -> oldData + (data as Map<String, Any?>)
            is Collection<*> -> oldData + (data as Collection<String>)
            else -> throw runtimeException
        }
        storage[topicType] = newData
    }

    fun remove(topicType: TopicType, topic: String) {
        val topicData = storage[topicType] ?: return

        val runtimeException = RuntimeException("Unsupported data $topicData for topic type $topicType")
        val filteredData = when (topicData) {
            is Map<*, *> -> topicData.filterKeys { it.toString() != topic }
            is Collection<*> -> topicData.filter { it.toString() != topic }
            else -> throw runtimeException
        }

        storage[topicType] = filteredData
    }

    fun getTopicType(topic: String) = TopicType.values()
            .find {
                val topicData = storage[it]
                when (topicData) {
                    is Map<*, *> -> topicData.containsKey(topic)
                    is Collection<*> -> topicData.contains(topic)
                    else -> false
                }
            }

    fun getTopics() = TopicType.values()
            .flatMap {
                val data = storage[it]
                when (data) {
                    is Map<*, *> -> data.keys
                    is Collection<*> -> data.toSet()
                    else -> emptySet<String>()
                }
            }.toSet() as Set<String>

    fun setAll(topics: Topics) {
        topics.asMap().forEach { topicType, data ->
            set(topicType, data)
        }
    }

    fun getCypherTemplate(topic: String) = (storage.getOrDefault(TopicType.CYPHER, emptyMap<String, String>()) as Map<String, String>)
            .let { it[topic] }

    fun getAll(): Map<TopicType, Any> = Collections.unmodifiableMap(storage)

}