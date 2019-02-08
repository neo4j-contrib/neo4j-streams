package streams

import org.apache.commons.lang3.StringUtils
import org.neo4j.kernel.impl.core.EmbeddedProxySPI
import org.neo4j.kernel.impl.core.GraphProperties
import org.neo4j.kernel.internal.GraphDatabaseAPI
import streams.service.TopicType
import streams.utils.Neo4jUtils


private const val STREAMS_TOPIC_KEY: String = "streams.sink.topic"
private const val STREAMS_TOPIC_KEY_CYPHER: String = "$STREAMS_TOPIC_KEY.cypher."
private const val STREAMS_TOPIC_KEY_CDC: String = "$STREAMS_TOPIC_KEY.cdc.merge"

class StreamsTopicService(private val db: GraphDatabaseAPI) {
    private val properties: GraphProperties = db.dependencyResolver.resolveDependency(EmbeddedProxySPI::class.java).newGraphPropertiesProxy()
    private val log = Neo4jUtils.getLogService(db).getUserLog(StreamsTopicService::class.java)

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
        val key = "$STREAMS_TOPIC_KEY_CYPHER$topic"
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
            properties.setProperty("$STREAMS_TOPIC_KEY_CYPHER$topic", query)
            it.success()
        }
    }

    fun setAllCypherTemplates(topics: Map<String, String>) {
        topics.forEach {
            setCypherTemplate(it.key, it.value)
        }
    }

    fun getCypherTemplate(topic: String): String? {
        val key = "$STREAMS_TOPIC_KEY_CYPHER$topic"
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
                    .filterKeys { it.startsWith(STREAMS_TOPIC_KEY_CYPHER) }
                    .map { it.key.replace(STREAMS_TOPIC_KEY_CYPHER, StringUtils.EMPTY) to it.value.toString()}
                    .toMap()
        }
    }

    fun setAllCDCTopics(topics: Set<String>) {
        if (!Neo4jUtils.isWriteableInstance(db) || topics.isEmpty()) {
            return
        }
        db.beginTx().use {
            properties.setProperty(STREAMS_TOPIC_KEY_CDC, topics.joinToString(";"))
            it.success()
        }
    }

    fun setCDCTopic(topic: String) {
        if (!Neo4jUtils.isWriteableInstance(db)) {
            return
        }
        db.beginTx().use {
            val cdcTopics = properties
                    .getProperty(STREAMS_TOPIC_KEY_CDC)
                    .toString()
                    .split(";")
                    .toMutableSet()
            cdcTopics += topic
            properties.setProperty(STREAMS_TOPIC_KEY_CDC, cdcTopics.joinToString(";"))
            it.success()
        }
    }

    fun getAllCDCTopics(): Set<String> {
        return db.beginTx().use {
            if (!properties.hasProperty(STREAMS_TOPIC_KEY_CDC)) {
                return emptySet()
            }
            return properties.getProperty(STREAMS_TOPIC_KEY_CDC, StringUtils.EMPTY)
                    .toString()
                    .split(";")
                    .toSet()
        }
    }

    fun getTopics(): Set<String> {
        return db.beginTx().use {
            val cypherTopics = getAllCypherTemplates().keys
            val cdcTopics = getAllCDCTopics()
            cypherTopics + cdcTopics
        }
    }

    fun getTopicType(topic: String): TopicType? {
        return if (getAllCDCTopics().contains(topic)) {
            TopicType.CDC_MERGE
        } else {
            db.beginTx().use {
                if (properties.hasProperty(STREAMS_TOPIC_KEY_CYPHER + topic)) TopicType.CYPHER else null
            }
        }
    }

}