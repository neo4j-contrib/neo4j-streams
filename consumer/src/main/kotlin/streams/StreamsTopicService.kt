package streams

import org.neo4j.graphdb.Label
import org.neo4j.kernel.internal.GraphDatabaseAPI
import streams.serialization.JSONUtils
import streams.service.STREAMS_TOPIC_KEY
import streams.service.TopicType
import streams.service.Topics
import streams.utils.Neo4jUtils
import java.lang.IllegalArgumentException

class StreamsTopicService(private val db: GraphDatabaseAPI) {

    init {
        if (db.databaseName() != Neo4jUtils.SYSTEM_DATABASE_NAME) {
            throw IllegalArgumentException("GraphDatabaseAPI must be an instance of ${Neo4jUtils.SYSTEM_DATABASE_NAME} database")
        }
    }

    private fun isEmpty(data: Any, excOnError: Exception) = when (data) {
        is Map<*, *> -> data.isEmpty()
        is Collection<*> -> data.isEmpty()
        else -> throw excOnError
    }

    fun clearAll() { // TODO move to Neo4jUtils#executeInWriteableInstance
        if (!Neo4jUtils.isWriteableInstance(db)) {
            return
        }
        return db.beginTx().use {
            it.findNodes(Label.label(STREAMS_TOPIC_KEY))
                    .forEach { it.delete() }
            it.commit()
        }
    }

    fun set(topicType: TopicType, data: Any) = Neo4jUtils.executeInWriteableInstance(db) {
        db.beginTx().use {
            val topicTypeLabel = Label.label(topicType.key)
            val findNodes = it.findNodes(topicTypeLabel)
            val node = if (findNodes.hasNext()) {
                findNodes.next()
            } else {
                it.createNode(Label.label(STREAMS_TOPIC_KEY), topicTypeLabel)
            }
            val runtimeException = RuntimeException("Unsupported data $data for topic type $topicType")
            val oldData = if (node.hasProperty("data")) {
                JSONUtils.readValue<Any>(node.getProperty("data"))
            } else {
                when (data) {
                    is Map<*, *> -> emptyMap<String, Any?>()
                    is Collection<*> -> emptyList<String>()
                    else -> throw runtimeException
                }
            }
            val newData = when (oldData) {
                is Map<*, *> -> oldData + (data as Map<String, Any?>)
                is Collection<*> -> oldData + (data as Collection<String>)
                else -> throw runtimeException
            }
            if (!isEmpty(newData, runtimeException)) {
                node.setProperty("data", JSONUtils.writeValueAsString(newData))
                it.commit()
            }
        }
    }

    fun remove(topicType: TopicType, topic: String) = Neo4jUtils.executeInWriteableInstance(db) {
        db.beginTx().use {
            val topicTypeLabel = Label.label(topicType.key)
            val findNodes = it.findNodes(topicTypeLabel)
            val node = if (findNodes.hasNext()) {
                findNodes.next()
            } else {
                return@executeInWriteableInstance
            }
            if (!node.hasProperty("data")) {
                return@executeInWriteableInstance
            }
            val topicData = JSONUtils.readValue<Any>(node.getProperty("data"))
            val runtimeException = RuntimeException("Unsupported data $topicData for topic type $topicType")
            val filteredData = when (topicData) {
                is Map<*, *> -> topicData.filterKeys { it.toString() != topic }
                is Collection<*> -> topicData.filter { it.toString() != topic }
                else -> throw runtimeException
            }
            if (isEmpty(filteredData, runtimeException)) {
                node.removeProperty("data")
            } else {
                node.setProperty("data", JSONUtils.writeValueAsString(filteredData))
            }
            it.commit()
        }
    }

    fun getTopicType(topic: String) = Neo4jUtils.executeInWriteableInstance(db) {
        db.beginTx().use { tx ->
            TopicType.values()
                    .find {
                        val topicTypeLabel = Label.label(it.key)
                        val findNodes = tx.findNodes(topicTypeLabel)
                        if (!findNodes.hasNext()) {
                            false
                        } else {
                            val node = findNodes.next()
                            val topicData = JSONUtils.readValue<Any>(node.getProperty("data"))
                            when (topicData) {
                                is Map<*, *> -> topicData.containsKey(topic)
                                is Collection<*> -> topicData.contains(topic)
                                else -> false
                            }
                        }
                    }
        }
    }

    fun getTopics() = db.beginTx().use { tx ->
        TopicType.values()
                .flatMap {
                    val topicTypeLabel = Label.label(it.key)
                    val findNodes = tx.findNodes(topicTypeLabel)
                    if (!findNodes.hasNext()) {
                        emptySet<String>()
                    } else {
                        val data = JSONUtils.readValue<Any>(findNodes.next().getProperty("data"))
                        when (data) {
                            is Map<*, *> -> data.keys
                            is Collection<*> -> data.toSet()
                            else -> emptySet<String>()
                        }
                    }
                }.toSet() as Set<String>
    }

    fun setAll(topics: Topics) {
        topics.asMap().forEach { topicType, data ->
            set(topicType, data)
        }
    }

    fun getCypherTemplate(topic: String) = db.beginTx().use {
        db.beginTx().use {
            val topicTypeLabel = Label.label(TopicType.CYPHER.key)
            val findNodes = it.findNodes(topicTypeLabel)
            if (!findNodes.hasNext()) {
                null
            } else {
                val node = findNodes.next()
                val data = if (node.hasProperty("data")) {
                    JSONUtils.readValue<Map<String, String>>(node.getProperty("data"))
                } else {
                    emptyMap()
                }
                data[topic]
            }
        }
    }

    fun getAll() = db.beginTx().use { tx ->
        TopicType.values()
                .mapNotNull {
                    val topicTypeLabel = Label.label(it.key)
                    val findNodes = tx.findNodes(topicTypeLabel)
                    if (!findNodes.hasNext()) {
                        null
                    } else {
                        val node = findNodes.next()
                        if (node.hasProperty("data")) {
                            it to JSONUtils.readValue<Any>(node.getProperty("data"))
                        } else {
                             null
                        }
                    }
                }
                .toMap()
    }

}