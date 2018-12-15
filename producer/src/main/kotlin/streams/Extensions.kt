package streams

import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import streams.events.EntityType
import streams.events.RelationshipNodeChange
import streams.extensions.*

fun Node.toMap(): Map<String, Any?> {
    return mapOf("id" to id.toString(), "properties" to allProperties, "labels" to labelNames(), "type" to EntityType.node)
}

fun Relationship.toMap(): Map<String, Any?> {
    return mapOf("id" to id.toString(), "properties" to allProperties, "label" to type,
            "start" to RelationshipNodeChange(startNode.id.toString(), startNode.labelNames()),
            "end" to RelationshipNodeChange(endNode.id.toString(), endNode.labelNames()),
            "type" to EntityType.relationship)
}