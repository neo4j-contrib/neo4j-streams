package streams

import org.apache.commons.lang3.StringUtils
import org.neo4j.kernel.impl.core.EmbeddedProxySPI
import org.neo4j.kernel.impl.core.GraphProperties
import org.neo4j.kernel.internal.GraphDatabaseAPI
import streams.service.STREAMS_TOPIC_CDC_KEY
import streams.service.STREAMS_TOPIC_KEY
import streams.service.TopicType
import streams.utils.Neo4jUtils

class StreamsTopicService(private val db: GraphDatabaseAPI) {
    private val properties: GraphProperties = db.dependencyResolver.resolveDependency(EmbeddedProxySPI::class.java).newGraphPropertiesProxy()
    private val log = Neo4jUtils.getLogService(db).getUserLog(StreamsTopicService::class.java)

    private val CYPHER_PREFIX: String = "${TopicType.CYPHER.key}."

    fun clearAll() {
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
        val key = "$CYPHER_PREFIX$topic"
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

    fun setCypherTemplate(topic: String, query: String) {
        if (!Neo4jUtils.isWriteableInstance(db)) {
            return
        }
        db.beginTx().use {
            properties.setProperty(CYPHER_PREFIX + topic, query)
            it.success()
        }
    }

    fun setAllCypherTemplates(topics: Map<String, String>) {
        topics.forEach {
            setCypherTemplate(it.key, it.value)
        }
    }

    fun getCypherTemplate(topic: String): String? {
        val key = "$CYPHER_PREFIX$topic"
        return db.beginTx().use {
            if (!properties.hasProperty(key)) {
                if (log.isDebugEnabled) {
                    log.debug("No query registered for topic $topic")
                }
                return null
            }
            return properties.getProperty(key).toString()
        }
    }

    fun getAllCypherTemplates(): Map<String, String> {
        return db.beginTx().use {
            return properties.allProperties
                    .filterKeys { it.startsWith(CYPHER_PREFIX) }
                    .map { it.key.replace(CYPHER_PREFIX, StringUtils.EMPTY) to it.value.toString()}
                    .toMap()
        }
    }

    fun setAllCDCTopics(topics: Map<TopicType, Set<String>>) {
        if (!Neo4jUtils.isWriteableInstance(db) || topics.isEmpty()) {
            return
        }
        db.beginTx().use {
            topics.forEach { type, topics ->
                if (topics.isNullOrEmpty()) return@forEach
                properties.setProperty(type.key, topics.joinToString(";"))
            }
            it.success()
        }
    }

    fun setCDCTopic(type: TopicType, topic: String) {
        if (!Neo4jUtils.isWriteableInstance(db)) {
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
                    .filter { it.isCDC }
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
            cypherTopics + cdcTopics
        }
    }

    fun getTopicType(topic: String): TopicType? {
        val type = getAllCDCTopics()
                .filterValues { it.contains(topic) }
                .map { it.key }
                .firstOrNull()
        return if (type != null) {
            type
        } else {
            db.beginTx().use {
                if (properties.hasProperty(CYPHER_PREFIX + topic)) TopicType.CYPHER else null
            }
        }
    }

}