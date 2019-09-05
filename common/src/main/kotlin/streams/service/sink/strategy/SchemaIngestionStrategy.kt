package streams.service.sink.strategy

import streams.events.*
import streams.extensions.quote
import streams.utils.IngestionUtils.getLabelsAsString
import streams.utils.IngestionUtils.getNodeKeysAsString
import streams.serialization.JSONUtils
import streams.service.StreamsSinkEntity
import streams.utils.SchemaUtils.getNodeKeys
import streams.utils.SchemaUtils.toStreamsTransactionEvent
import streams.utils.StreamsUtils


private fun prepareRelationshipEvents(events: List<StreamsTransactionEvent>, withProperties: Boolean = true): Map<RelationshipSchemaMetadata, List<Map<String, Any>>> = events
        .mapNotNull {
            val payload = it.payload as RelationshipPayload

            if (payload.start.ids.isEmpty() || payload.end.ids.isEmpty()) {
                null
            } else {
                val properties = payload.after?.properties ?: payload.before?.properties ?: emptyMap()

                val key = RelationshipSchemaMetadata(it.payload)
                val value = if (withProperties) {
                    mapOf("start" to payload.start.ids, "end" to payload.end.ids, "properties" to properties)
                } else {
                    mapOf("start" to payload.start.ids, "end" to payload.end.ids)
                }

                key to value
            }
        }
        .groupBy { it.first }
        .mapValues { it.value.map { it.second } }

class SchemaIngestionStrategy: IngestionStrategy {

    override fun mergeRelationshipEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
        return prepareRelationshipEvents(events
                    .mapNotNull { toStreamsTransactionEvent(it) { it.payload.type == EntityType.relationship
                            && it.meta.operation != OperationType.deleted } })
                .map {
                    val label = it.key.label.quote()
                    val query = """
                        |${StreamsUtils.UNWIND}
                        |MERGE (start${getLabelsAsString(it.key.startLabels)}{${getNodeKeysAsString("start", it.key.startKeys)}})
                        |MERGE (end${getLabelsAsString(it.key.endLabels)}{${getNodeKeysAsString("end", it.key.endKeys)}})
                        |MERGE (start)-[r:$label]->(end)
                        |SET r = event.properties
                    """.trimMargin()
                    QueryEvents(query, it.value)
                }
    }

    override fun deleteRelationshipEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
        return prepareRelationshipEvents(events
                    .mapNotNull { toStreamsTransactionEvent(it) { it.payload.type == EntityType.relationship
                            && it.meta.operation == OperationType.deleted } }, false)
                .map {
                    val label = it.key.label.quote()
                    val query = """
                        |${StreamsUtils.UNWIND}
                        |MATCH (start${getLabelsAsString(it.key.startLabels)}{${getNodeKeysAsString("start", it.key.startKeys)}})
                        |MATCH (end${getLabelsAsString(it.key.endLabels)}{${getNodeKeysAsString("end", it.key.endKeys)}})
                        |MATCH (start)-[r:$label]->(end)
                        |DELETE r
                    """.trimMargin()
                    QueryEvents(query, it.value)
                }
    }

    override fun deleteNodeEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
        return events
                .mapNotNull { toStreamsTransactionEvent(it) { it.payload.type == EntityType.node && it.meta.operation == OperationType.deleted } }
                .mapNotNull {
                    val changeEvtBefore = it.payload.before as NodeChange
                    val constraints = getNodeConstraints(it) { it.type == StreamsConstraintType.UNIQUE }
                    if (constraints.isEmpty()) {
                        null
                    } else {
                        constraints to mapOf("properties" to changeEvtBefore.properties)
                    }
                }
                .groupBy({ it.first }, { it.second })
                .map {
                    val labels = it.key.mapNotNull { it.label }
                    val nodeKeys = it.key.flatMap { it.properties }.toSet()
                    val query = """
                        |${StreamsUtils.UNWIND}
                        |MATCH (n${getLabelsAsString(labels)}{${getNodeKeysAsString(keys = nodeKeys)}})
                        |DETACH DELETE n
                    """.trimMargin()
                    QueryEvents(query, it.value)
                }
    }

    override fun mergeNodeEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
        val filterLabels: (List<String>, List<Constraint>) -> List<String> = { labels, constraints ->
            labels.filter { label -> !constraints.any { constraint -> constraint.label == label } }
                .map { it.quote() }
        }
        return events
                .mapNotNull { toStreamsTransactionEvent(it) { it.payload.type == EntityType.node && it.meta.operation != OperationType.deleted } }
                .mapNotNull {
                    val changeEvtAfter = it.payload.after as NodeChange
                    val labelsAfter = changeEvtAfter.labels ?: emptyList()
                    val labelsBefore = (it.payload.before as? NodeChange)?.labels.orEmpty()

                    val constraints = getNodeConstraints(it) { it.type == StreamsConstraintType.UNIQUE }
                    if (constraints.isEmpty()) {
                        null
                    } else {
                        val labelsToAdd = filterLabels((labelsAfter - labelsBefore), constraints)
                        val labelsToDelete = filterLabels((labelsBefore - labelsAfter), constraints)

                        val propertyKeys = changeEvtAfter.properties?.keys ?: emptySet()
                        val keys = getNodeKeys(labelsAfter, propertyKeys, constraints)

                        if (keys.isEmpty()) {
                            null
                        } else {
                            val key = NodeSchemaMetadata(constraints = constraints,
                                    labelsToAdd = labelsToAdd, labelsToDelete = labelsToDelete,
                                    keys = keys)
                            val value = mapOf("properties" to changeEvtAfter.properties)
                            key to value
                        }
                    }
                }
                .groupBy({ it.first }, { it.second })
                .map { map ->
                    var query = """
                        |${StreamsUtils.UNWIND}
                        |MERGE (n${getLabelsAsString(map.key.constraints.mapNotNull { it.label })}{${getNodeKeysAsString(keys = map.key.keys)}})
                        |SET n = event.properties
                    """.trimMargin()
                    if (map.key.labelsToAdd.isNotEmpty()) {
                        query += "\nSET n${getLabelsAsString(map.key.labelsToAdd)}"
                    }
                    if (map.key.labelsToDelete.isNotEmpty()) {
                        query += "\nREMOVE n${getLabelsAsString(map.key.labelsToDelete)}"
                    }
                    QueryEvents(query, map.value)
                }
    }

    private fun getNodeConstraints(event: StreamsTransactionEvent,
                                   filter: (Constraint) -> Boolean): List<Constraint> = event.schema.constraints.filter { filter(it) }

}