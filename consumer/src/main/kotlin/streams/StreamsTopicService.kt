package streams

import org.apache.commons.lang3.StringUtils
import org.neo4j.kernel.impl.core.EmbeddedProxySPI
import org.neo4j.kernel.impl.core.GraphProperties
import org.neo4j.kernel.internal.GraphDatabaseAPI
import streams.serialization.JSONUtils
import streams.service.STREAMS_TOPIC_KEY
import streams.service.TopicType
import streams.service.TopicTypeGroup
import streams.service.sink.strategy.NodePatternConfiguration
import streams.service.sink.strategy.RelationshipPatternConfiguration
import streams.utils.Neo4jUtils
import streams.service.TopicUtils
import streams.service.Topics

class StreamsTopicService(private val db: GraphDatabaseAPI) {
    private val properties: GraphProperties = db.dependencyResolver.resolveDependency(EmbeddedProxySPI::class.java).newGraphPropertiesProxy()

    fun clearAll() { // TODO move to Neo4jUtils#executeInWriteableInstance
        if (!Neo4jUtils.isWriteableInstance(db)) {
            return
        }
        return db.beginTx().use {
            val keys = properties.allProperties
                    .filterKeys { it.startsWith(STREAMS_TOPIC_KEY) }
                    .keys
            keys.forEach {
                properties.removeProperty(it)
            }
            it.success()
        }
    }

    fun set(topicType: TopicType, data: Any) = Neo4jUtils.executeInWriteableInstance(db) {
        db.beginTx().use {
            if (properties.hasProperty(topicType.key)) {
                val topicData = JSONUtils.readValue<Any>(properties.getProperty(topicType.key))
                val newData = when (topicData) {
                    is Map<*, *> -> topicData + (data as Map<String, Any?>)
                    is Collection<*> -> topicData + (data as Collection<String>)
                    else -> throw RuntimeException("Unsupported data $data for topic type $topicType")
                }
                properties.setProperty(topicType.key, JSONUtils.writeValueAsString(newData))
            } else {
                properties.setProperty(topicType.key, JSONUtils.writeValueAsString(data))
            }
            it.success()
        }
    }

    fun remove(topicType: TopicType, topic: String) = Neo4jUtils.executeInWriteableInstance(db) {
        db.beginTx().use {
            if (properties.hasProperty(topicType.key)) {
                val topicData = JSONUtils.readValue<Any>(properties.getProperty(topicType.key))
                val newData = when (topicData) {
                    is Map<*, *> -> topicData.filterKeys { it.toString() != topic }
                    is Collection<*> -> topicData.filter { it.toString() != topic }
                    else -> throw RuntimeException("Unsupported data $topicData for topic type $topicType")
                }
                val isEmpty = when (newData) {
                    is Map<*, *> -> newData.isEmpty()
                    is Collection<*> -> newData.isEmpty()
                    else -> throw RuntimeException("Unsupported data $topicData for topic type $topicType")
                }
                if (isEmpty) {
                    properties.removeProperty(topicType.key)
                } else {
                    properties.setProperty(topicType.key, JSONUtils.writeValueAsString(newData))
                }
            }
            it.success()
        }
    }

    fun getTopicType(topic: String) = Neo4jUtils.executeInWriteableInstance(db) {
        db.beginTx().use {
            TopicType.values().find {
                if (!properties.hasProperty(it.key)) {
                    false
                } else {
                    val data = JSONUtils.readValue<Any>(properties.getProperty(it.key))
                    when (data) {
                        is Map<*, *> -> data.containsKey(topic)
                        is Collection<*> -> data.contains(topic)
                        else -> false
                    }
                }
            }
        }
    }

    fun getTopics() = db.beginTx().use {
        TopicType.values()
                .filter { properties.hasProperty(it.key) }
                .flatMap {
                    val data = JSONUtils.readValue<Any>(properties.getProperty(it.key))
                    when (data) {
                        is Map<*, *> -> data.keys
                        is Collection<*> -> data.toSet()
                        else -> emptySet()
                    }
                }.toSet() as Set<String>
    }

    fun setAll(topics: Topics) {
        topics.asMap().forEach { topicType, data ->
            set(topicType, data)
        }
    }

    fun getCypherTemplate(topic: String) = db.beginTx().use {
        if (properties.hasProperty(TopicType.CYPHER.key)) {
            val data = JSONUtils.readValue<Map<String, String>>(properties.getProperty(TopicType.CYPHER.key))
            data[topic]
        } else {
            null
        }
    }

    fun getAll() = db.beginTx().use {
        TopicType.values()
                .filter { properties.hasProperty(it.key) }
                .map { it to JSONUtils.readValue<Any>(properties.getProperty(it.key)) }
                .toMap()
    }

}