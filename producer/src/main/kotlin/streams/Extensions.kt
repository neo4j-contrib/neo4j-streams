package streams

import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.config.TopicConfig
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.schema.ConstraintDefinition
import org.neo4j.graphdb.schema.ConstraintType
import streams.events.*
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

fun RecordMetadata.toMap(): Map<String, Any> = mapOf(
        "offset" to offset(),
        "timestamp" to timestamp(),
        "keySize" to serializedKeySize(),
        "valueSize" to serializedValueSize(),
        "partition" to partition()
)

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

fun StreamsTransactionEvent.asSourceRecordValue(strategy: String): StreamsTransactionEvent? =
        if(!isStrategyDelete(strategy) && meta.operation == OperationType.deleted) null else this

fun StreamsTransactionEvent.asSourceRecordKey(strategy: String): Any =
        if (isStrategyDelete(strategy)) {
         "${meta.txId + meta.txEventId}-${meta.txEventId}"
        } else {
            if (payload is NodePayload) {
                val payload = payload as NodePayload
                getNodeConstraintsByLabels(payload.after?.labels ?: payload.before?.labels, schema.constraints, payload.id, payload.after?.properties ?: payload.before?.properties)
            } else  {
                val payload = payload as RelationshipPayload
                mapOf(
                        "start" to getNodeConstraintsByLabels(payload.start.labels, schema.constraints, payload.start.id,  payload.start.ids),
                        "end" to getNodeConstraintsByLabels(payload.end.labels, schema.constraints, payload.end.id, payload.end.ids),
                        "id" to payload.id
                )
            }
        }

fun getNodeConstraintsByLabels(labels: List<String>?, constraints: List<Constraint>, id: String,  props: Map<*,*>?): Any =
        if(labels.isNullOrEmpty() || props.isNullOrEmpty()) {
            id
        } else {
            val constraintFiltered = constraints.sortedBy { it.label }.filter { labels.contains(it.label) }
            if (constraintFiltered.isNotEmpty()) {
                val propsConstraint: Set<String> = constraintFiltered.flatMap { it.properties }.toSet()
                props.filterKeys { it in propsConstraint }
            } else {
                id
            }
        }

fun isStrategyDelete(strategy: String) = strategy == TopicConfig.CLEANUP_POLICY_DELETE