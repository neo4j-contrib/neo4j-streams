package streams

import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import streams.events.EntityType
import streams.events.RelationshipNodeChange

fun Map<String,String>.getInt(name:String, defaultValue: Int) = this.get(name)?.toInt() ?: defaultValue

fun Node.toMap(): Map<String, Any?> {
    return mapOf("id" to id.toString(), "properties" to allProperties, "labels" to labelNames(), "type" to EntityType.node)
}

fun Relationship.toMap(): Map<String, Any?> {
    return mapOf("id" to id.toString(), "properties" to allProperties, "label" to type,
            "start" to RelationshipNodeChange(startNode.id.toString(), startNode.labelNames()),
            "end" to RelationshipNodeChange(endNode.id.toString(), endNode.labelNames()),
            "type" to EntityType.relationship)
}

fun Node.labelNames() : List<String> {
    return this.labels.map { it.name() }
}

fun String.toPointCase(): String {
    return this.split("(?<=[a-z])(?=[A-Z])".toRegex()).joinToString(separator = ".").toLowerCase()
}