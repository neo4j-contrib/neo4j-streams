package streams.extensions

import org.neo4j.driver.types.Node
import org.neo4j.driver.types.Relationship
import java.util.*
import javax.lang.model.SourceVersion

fun Map<*, *>.asProperties() = this.let {
    val properties = Properties()
    properties.putAll(it)
    properties
}

fun Node.asStreamsMap(): Map<String, Any?> {
    val nodeMap = this.asMap().toMutableMap()
    nodeMap["<id>"] = this.id()
    nodeMap["<labels>"] = this.labels()
    return nodeMap
}

fun Relationship.asStreamsMap(): Map<String, Any?> {
    val relMap = this.asMap().toMutableMap()
    relMap["<id>"] = this.id()
    relMap["<type>"] = this.type()
    relMap["<source.id>"] = this.startNodeId()
    relMap["<target.id>"] = this.endNodeId()
    return relMap
}

fun String.quote(): String = if (SourceVersion.isIdentifier(this)) this else "`$this`"

fun Map<String, Any?>.flatten(map: Map<String, Any?> = this, prefix: String = ""): Map<String, Any?> {
    return map.flatMap {
        val key = it.key
        val value = it.value
        val newKey = if (prefix != "") "$prefix.$key" else key
        if (value is Map<*, *>) {
            flatten(value as Map<String, Any>, newKey).toList()
        } else {
            listOf(newKey to value)
        }
    }.toMap()
}