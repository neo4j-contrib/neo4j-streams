package streams

import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.schema.ConstraintDefinition
import org.neo4j.graphdb.schema.ConstraintType
import streams.events.EntityType
import streams.events.StreamsConstraintType
import streams.extensions.labelNames

fun Node.toMap(): Map<String, Any?> {
    return mapOf("id" to id.toString(), "properties" to allProperties, "labels" to labelNames(), "type" to EntityType.node)
}

fun Relationship.toMap(): Map<String, Any?> {
    return mapOf("id" to id.toString(), "properties" to allProperties, "label" to type,
            "start" to startNode.toMap(),
            "end" to endNode.toMap(),
            "type" to EntityType.relationship)
}

fun ConstraintDefinition.streamsConstraintType(): StreamsConstraintType {
    return when (this.constraintType) {
        ConstraintType.UNIQUENESS, ConstraintType.NODE_KEY -> StreamsConstraintType.UNIQUE
        else -> if (isNodeConstraint()) StreamsConstraintType.NODE_PROPERTY_EXISTS else StreamsConstraintType.RELATIONSHIP_PROPERTY_EXISTS
    }
}

fun ConstraintDefinition.isNodeConstraint(): Boolean {
    return try { this.label; true } catch (e: IllegalStateException) { false }
}

fun ConstraintDefinition.isRelationshipConstraint(): Boolean {
    return try { this.relationshipType; true } catch (e: IllegalStateException) { false }
}