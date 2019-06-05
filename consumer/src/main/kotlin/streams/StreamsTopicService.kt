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
    private val log = Neo4jUtils.getLogService(db).getUserLog(StreamsTopicService::class.java)

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

    fun remove(topic: String) {
        if (!Neo4jUtils.isWriteableInstance(db)) {
            return
        }
        val key = "${TopicType.CYPHER.key}.$topic"
        return db.beginTx().use {
            if (!properties.hasProperty(key)) {
                if (log.isDebugEnabled) {
                    log.debug("No query registered for topic $topic")
                }
                return
            }
            properties.removeProperty(key)
            it.success()
        }
    }

    fun setAllCypherTemplates(topics: Map<String, String>) {
        setAllTopicValue(TopicType.CYPHER.key, topics)
    }

    fun getCypherTemplate(topic: String): String? {
        return getTopicValue(TopicType.CYPHER.key, topic)
    }

    private fun getTopicValue(key: String, topic: String): String? {
        val fullKey = "$key.$topic"
        return  db.beginTx().use {
            if (!properties.hasProperty(fullKey)) {
                if (log.isDebugEnabled) {
                    log.debug("No configuration registered for topic $topic")
                }
                return null
            }
            return properties.getProperty(fullKey).toString()
        }
    }

    fun getAllCypherTemplates(): Map<String, String> {
        val prefix = "${TopicType.CYPHER.key}."
        return getAllTopicValue(prefix)
    }

    fun setAllCDCTopicsByType(type: TopicType, topics: Set<String>) {
        if (!Neo4jUtils.isWriteableInstance(db) || topics.isEmpty() || type.group != TopicTypeGroup.CDC) {
            return
        }
        db.beginTx().use {
            properties.setProperty(type.key, topics.joinToString(TopicUtils.CDC_TOPIC_SEPARATOR))
            it.success()
        }
    }

    fun setCDCTopic(type: TopicType, topic: String) {
        if (!Neo4jUtils.isWriteableInstance(db)) {
            return
        }
        if (type.group != TopicTypeGroup.CDC) {
            return
        }
        db.beginTx().use {
            val cdcTopics = if (properties.hasProperty(type.key)) {
                properties
                    .getProperty(type.key)
                    .toString()
                    .split(";")
                    .toMutableSet()
            } else {
                mutableSetOf()
            }
            cdcTopics += topic
            properties.setProperty(type.key, cdcTopics.joinToString(";"))
            it.success()
        }
    }

    fun getAllCDCTopics(): Map<TopicType, Set<String>> {
        return db.beginTx().use {
            TopicType.values()
                    .filter { it.group == TopicTypeGroup.CDC }
                    .map {
                        if (!properties.hasProperty(it.key)) {
                            it to emptySet()
                        } else {
                            it to properties.getProperty(it.key, StringUtils.EMPTY)
                                    .toString()
                                    .split(";")
                                    .toSet()
                        }
                    }
                    .associateBy({ it.first }, { it.second })
        }
    }

    fun getTopics(): Set<String> {
        return db.beginTx().use {
            val cypherTopics = getAllCypherTemplates().keys
            val cdcTopics = getAllCDCTopics().flatMap { it.value }.toSet()
            val nodePatternTopics = getAllNodePatternTopics().keys
            val relPatternTopics = getAllRelPatternTopics().keys
            cypherTopics + cdcTopics + nodePatternTopics + relPatternTopics
        }
    }

    fun getTopicType(topic: String): TopicType? {
        return getAllCDCTopics()
                .filterValues { it.contains(topic) }
                .map { it.key }
                .firstOrNull()
                ?:
                db.beginTx().use {
                    return TopicType.values()
                            .filter { topicType ->
                                if (topicType.group == TopicTypeGroup.CDC) {
                                    false
                                }
                                properties.hasProperty("${topicType.key}.$topic")
                            }
                            .firstOrNull()
                }
    }

    fun setAll(topics: Topics) {
        topics.asMap().forEach { topicType, data ->
            when (topicType) {
                TopicType.CYPHER -> setAllCypherTemplates(data as Map<String, String>)
                TopicType.CDC_SCHEMA, TopicType.CDC_SOURCE_ID -> setAllCDCTopicsByType(topicType, data as Set<String>)
                TopicType.PATTERN_NODE -> setAllNodePatternTopics(data as Map<String, NodePatternConfiguration>)
                TopicType.PATTERN_RELATIONSHIP -> setAllRelPatternTopics(data as Map<String, RelationshipPatternConfiguration>)
            }
        }
    }

    private fun setAllTopicValue(prefix: String, nodePatternTopics: Map<String, Any>) {
        if (!Neo4jUtils.isWriteableInstance(db)) {
            return
        }
        db.beginTx().use {
            nodePatternTopics.forEach { topic, config ->
                properties.setProperty("$prefix.$topic", if (config is String) config else JSONUtils.writeValueAsString(config))
            }
            it.success()
        }
    }

    fun getRelPattern(topic: String): RelationshipPatternConfiguration? {
        val cfgString = getTopicValue(TopicType.PATTERN_RELATIONSHIP.key, topic) ?: return null
        return JSONUtils.readValue<RelationshipPatternConfiguration>(cfgString)
    }

    fun getNodePattern(topic: String): NodePatternConfiguration? {
        val cfgString = getTopicValue(TopicType.PATTERN_NODE.key, topic) ?: return null
        return JSONUtils.readValue<NodePatternConfiguration>(cfgString)
    }

    fun setAllNodePatternTopics(nodePatternTopics: Map<String, NodePatternConfiguration>) {
        setAllTopicValue(TopicType.PATTERN_NODE.key, nodePatternTopics)
    }

    fun setAllRelPatternTopics(relPatternTopics: Map<String, RelationshipPatternConfiguration>) {
        setAllTopicValue(TopicType.PATTERN_RELATIONSHIP.key, relPatternTopics)
    }

    fun getAllNodePatternTopics(): Map<String, String> {
        val prefix = "${TopicType.PATTERN_NODE.key}."
        return getAllTopicValue(prefix)
    }

    fun getAllRelPatternTopics(): Map<String, String> {
        val prefix = "${TopicType.PATTERN_RELATIONSHIP.key}."
        return getAllTopicValue(prefix)
    }

    private fun getAllTopicValue(prefix: String): Map<String, String> {
        return db.beginTx().use {
            return properties.allProperties
                    .filterKeys { it.startsWith(prefix) }
                    .map { it.key.replace(prefix, StringUtils.EMPTY) to it.value.toString() }
                    .toMap()
        }
    }

}