package streams

import org.apache.commons.lang3.StringUtils
import org.neo4j.kernel.impl.core.EmbeddedProxySPI
import org.neo4j.kernel.impl.core.GraphProperties
import org.neo4j.kernel.impl.logging.LogService
import org.neo4j.kernel.internal.GraphDatabaseAPI
import streams.utils.Neo4jUtils


private const val STREAMS_TOPIC_KEY: String = "streams.sink.topic."

class StreamsTopicService(private val db: GraphDatabaseAPI, private val streamsSinkConfiguration: StreamsSinkConfiguration) {
    private val properties: GraphProperties = db.dependencyResolver.resolveDependency(EmbeddedProxySPI::class.java).newGraphPropertiesProxy()
    private val log = Neo4jUtils.getLogService(db).getUserLog(StreamsTopicService::class.java)

    init {
        setAll(streamsSinkConfiguration.topics)
    }

    fun set(topic: String, query: String) {
        if (!Neo4jUtils.isWriteableInstance(db)) {
            return
        }
        db.beginTx().use {
            properties.setProperty("$STREAMS_TOPIC_KEY$topic", query)
            it.success()
        }
    }

    fun setAll(topics: Map<String, String>) {
        topics.forEach {
            set(it.key, it.value)
        }
    }

    fun get(topic: String): String? {
        val key = "$STREAMS_TOPIC_KEY$topic"
        return db.beginTx().use {
            if (!properties.hasProperty(key)) {
                log.debug("No query registered for topic $topic")
                return null
            }
            return properties.getProperty(key).toString()
        }
    }

    fun getAll(): Map<String, String> {
        return db.beginTx().use {
            return properties.allProperties
                    .filterKeys { it.startsWith(STREAMS_TOPIC_KEY) }
                    .map { it.key.replace(STREAMS_TOPIC_KEY, StringUtils.EMPTY) to it.value.toString()}
                    .toMap()
        }
    }

    fun getTopics(): Set<String> {
        return db.beginTx().use {
            return properties.propertyKeys
                    .filter { it.startsWith(STREAMS_TOPIC_KEY) }
                    .map { it.replace(STREAMS_TOPIC_KEY, StringUtils.EMPTY) }
                    .toSet()
        }
    }

}