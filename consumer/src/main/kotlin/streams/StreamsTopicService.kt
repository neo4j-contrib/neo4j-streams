package streams

import org.neo4j.graphdb.Label
import org.neo4j.kernel.internal.GraphDatabaseAPI
import streams.serialization.JSONUtils
import streams.service.STREAMS_TOPIC_KEY
import streams.service.TopicType
import streams.service.Topics
import streams.utils.Neo4jUtils

class StreamsTopicService(private val db: GraphDatabaseAPI) {

    fun clearAll() { // TODO move to Neo4jUtils#executeInWriteableInstance
        if (!Neo4jUtils.isWriteableInstance(db)) {
            return
        }
        return db.beginTx().use {
            it.findNodes(Label.label(STREAMS_TOPIC_KEY))
                    .forEach { it.delete() }
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
            val oldData = JSONUtils.readValue<Any>(node.getProperty("data"))
            val newData = when (oldData) {
                is Map<*, *> -> oldData + (data as Map<String, Any?>)
                is Collection<*> -> oldData + (data as Collection<String>)
                else -> throw RuntimeException("Unsupported data $data for topic type $topicType")
            }
            node.setProperty("data", newData)
//            if (properties.hasProperty(topicType.key)) {
//                val topicData = JSONUtils.readValue<Any>(properties.getProperty(topicType.key))
//                val newData = when (topicData) {
//                    is Map<*, *> -> topicData + (data as Map<String, Any?>)
//                    is Collection<*> -> topicData + (data as Collection<String>)
//                    else -> throw RuntimeException("Unsupported data $data for topic type $topicType")
//                }
//                properties.setProperty(topicType.key, JSONUtils.writeValueAsString(newData))
//            } else {
//                properties.setProperty(topicType.key, JSONUtils.writeValueAsString(data))
//            }
//            it.success()
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
            val topicData = JSONUtils.readValue<Any>(node.getProperty("data"))
            val filteredData = when (topicData) {
                is Map<*, *> -> topicData.filterKeys { it.toString() != topic }
                is Collection<*> -> topicData.filter { it.toString() != topic }
                else -> throw RuntimeException("Unsupported data $topicData for topic type $topicType")
            }
            val isEmpty = when (filteredData) {
                is Map<*, *> -> filteredData.isEmpty()
                is Collection<*> -> filteredData.isEmpty()
                else -> throw RuntimeException("Unsupported data $topicData for topic type $topicType")
            }
            if (isEmpty) {
                node.removeProperty(topicType.key)
            } else {
                node.setProperty(topicType.key, filteredData)
            }
//            if (properties.hasProperty(topicType.key)) {
//                val topicData = JSONUtils.readValue<Any>(properties.getProperty(topicType.key))
//                val newData = when (topicData) {
//                    is Map<*, *> -> topicData.filterKeys { it.toString() != topic }
//                    is Collection<*> -> topicData.filter { it.toString() != topic }
//                    else -> throw RuntimeException("Unsupported data $topicData for topic type $topicType")
//                }
//                val isEmpty = when (newData) {
//                    is Map<*, *> -> newData.isEmpty()
//                    is Collection<*> -> newData.isEmpty()
//                    else -> throw RuntimeException("Unsupported data $topicData for topic type $topicType")
//                }
//                if (isEmpty) {
//                    properties.removeProperty(topicType.key)
//                } else {
//                    properties.setProperty(topicType.key, JSONUtils.writeValueAsString(newData))
//                }
//            }
//            it.success()
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
//                    .find {
//                        if (!properties.hasProperty(it.key)) {
//                            false
//                        } else {
//                            val data = JSONUtils.readValue<Any>(properties.getProperty(it.key))
//                            when (data) {
//                                is Map<*, *> -> data.containsKey(topic)
//                                is Collection<*> -> data.contains(topic)
//                                else -> false
//                            }
//                        }
//                    }
        }
    }

    fun getTopics() = db.beginTx().use { tx ->
        TopicType.values()
                .flatMap {
                    val topicTypeLabel = Label.label(it.key)
                    val findNodes = tx.findNodes(topicTypeLabel)
                    if (!findNodes.hasNext()) {
                        emptySet<Any>()
                    } else {
                        val data = JSONUtils.readValue<Any>(findNodes.next().getProperty("data"))
                        when (data) {
                            is Map<*, *> -> data.keys
                            is Collection<*> -> data.toSet()
                            else -> emptySet()
                        }
                    }

                }.toSet() as Set<String>
//        TopicType.values()
//                .filter { properties.hasProperty(it.key) }
//                .flatMap {
//                    val data = JSONUtils.readValue<Any>(properties.getProperty(it.key))
//                    when (data) {
//                        is Map<*, *> -> data.keys
//                        is Collection<*> -> data.toSet()
//                        else -> emptySet()
//                    }
//                }.toSet() as Set<String>
    }

    fun setAll(topics: Topics) {
        topics.asMap().forEach { topicType, data ->
            set(topicType, data)
        }
    }

    fun getCypherTemplate(topic: String) = db.beginTx().use {
//        if (properties.hasProperty(TopicType.CYPHER.key)) {
//            val data = JSONUtils.readValue<Map<String, String>>(properties.getProperty(TopicType.CYPHER.key))
//            data[topic]
//        } else {
//            null
//        }
        db.beginTx().use {
            val topicTypeLabel = Label.label(TopicType.CYPHER.key)
            val findNodes = it.findNodes(topicTypeLabel)
            if (!findNodes.hasNext()) {
                null
            } else {
                val data = JSONUtils.readValue<Map<String, String>>(findNodes.next().getProperty("data"))
                data[topic]
            }
        }
    }

    fun getAll() = db.beginTx().use { tx ->
        TopicType.values()
                .mapNotNull {
                    val topicTypeLabel = Label.label(TopicType.CYPHER.key)
                    val findNodes = tx.findNodes(topicTypeLabel)
                    if (!findNodes.hasNext()) {
                        null
                    } else {
                        it to JSONUtils.readValue<Any>(findNodes.next().getProperty(it.key))
                    }
                }
                .toMap()
//        TopicType.values()
//                .filter { properties.hasProperty(it.key) }
//                .map { it to JSONUtils.readValue<Any>(properties.getProperty(it.key)) }
//                .toMap()
    }

}