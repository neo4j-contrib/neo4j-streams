package streams

import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.config.TopicConfig
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.schema.ConstraintDefinition
import org.neo4j.graphdb.schema.ConstraintType
import streams.events.*
import streams.extensions.labelNames
import java.lang.RuntimeException

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
        if(isStrategyCompact(strategy) && meta.operation == OperationType.deleted) null else this

fun StreamsTransactionEvent.asSourceRecordKey(strategy: String): Any =
        when {
            strategy == TopicConfig.CLEANUP_POLICY_DELETE -> "${meta.txId + meta.txEventId}-${meta.txEventId}"
            isStrategyCompact(strategy) && payload is NodePayload -> getKeyOfNodeWithCompact(payload as NodePayload, schema)
            isStrategyCompact(strategy) && payload is RelationshipPayload -> getKeyOfRelWithCompact(payload as RelationshipPayload)
            else -> {
                throw RuntimeException("Invalid kafka.streams.log.compaction.strategy value: $strategy")
            }
        }

fun getKeyOfNodeWithCompact(payload: NodePayload, schema: Schema ): Any {
    val props: Map<String, Any> = payload.after?.properties ?: payload.before?.properties ?: emptyMap()

    return schema.constraints
            .flatMap { it.properties }
            .sorted()
            .firstOrNull {
                props.containsKey(it)
            }?.let { mapOf(it to props[it]) }
            .orEmpty()
            .ifEmpty { payload.id }
}

fun getKeyOfRelWithCompact(payload: RelationshipPayload ): Any = mapOf(
            "start" to payload.start.ids.ifEmpty { payload.start.id },
            "end" to payload.end.ids.ifEmpty { payload.end.id },
            "id" to payload.id
    )

fun isStrategyCompact(strategy: String) = strategy == TopicConfig.CLEANUP_POLICY_COMPACT