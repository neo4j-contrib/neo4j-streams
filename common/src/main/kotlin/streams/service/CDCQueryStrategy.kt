package streams.service

import org.neo4j.graphdb.schema.ConstraintType
import streams.events.*
import streams.utils.StreamsUtils

private const val backtickSeparator = "`:`"
private const val keySeparator = ", "

data class QueryEvents(val query: String, val events: List<Map<String, Any?>>)

interface CDCQueryStrategy {
    fun mergeNodeEvents(events: List<StreamsTransactionEvent>): List<QueryEvents>
    fun deleteNodeEvents(events: List<StreamsTransactionEvent>): List<QueryEvents>
    fun mergeRelationshipEvents(events: List<StreamsTransactionEvent>): List<QueryEvents>
    fun deleteRelationshipEvents(events: List<StreamsTransactionEvent>): List<QueryEvents>
}

data class RelationshipSchemaMetadata(val label: String,
                                      val startLabels: List<String>,
                                      val endLabels: List<String>,
                                      val startKeys: Set<String>,
                                      val endKeys: Set<String>)

data class NodeSchemaMetadata(val constraints: List<Constraint>,
                              val labelsToAdd: Set<String>,
                              val labelsToDelete: Set<String>,
                              val keys: Set<String>)

class SchemaCDCQueryStrategy: CDCQueryStrategy {

    override fun mergeRelationshipEvents(events: List<StreamsTransactionEvent>): List<QueryEvents> {
        if (events.isNullOrEmpty()) {
            return emptyList()
        }
        return events
                .filter { it.payload.type == EntityType.relationship && it.meta.operation != OperationType.deleted }
                .map {
                    val payload = it.payload as RelationshipPayload

                    if (payload.start.ids.isEmpty() || payload.end.ids.isEmpty()) {
                        throw RuntimeException("Node must have constraints defined in order to use the Schema Merge option")
                    }

                    val properties = payload.after?.properties ?: payload.before?.properties ?: emptyMap()

                    val key = RelationshipSchemaMetadata(label = it.payload.label,
                            startLabels = payload.start.labels.orEmpty(),
                            endLabels = payload.end.labels.orEmpty(),
                            startKeys = payload.start.ids.keys,
                            endKeys = payload.end.ids.keys)
                    val value = mapOf("start" to payload.start.ids, "end" to payload.end.ids, "properties" to properties)

                   key to value
                }
                .groupBy { it.first }
                .mapValues { it.value.map { it.second } }
                .map {
                    val label = it.key.label
                    val startLabels = it.key.startLabels.joinToString(backtickSeparator)
                    val endLabels = it.key.endLabels.joinToString(backtickSeparator)
                    val startNodeKeys = it.key.startKeys.map { "`${it}`: event.start.`${it}`" }
                            .joinToString(keySeparator)
                    val endNodeKeys = it.key.endKeys.map { "`${it}`: event.end.`${it}`" }
                            .joinToString(keySeparator)
                    val query = """
                        |${StreamsUtils.UNWIND}
                        |MERGE (start:`$startLabels`{$startNodeKeys})
                        |MERGE (end:`$endLabels`{$endNodeKeys})
                        |MERGE (start)-[r:`$label`]->(end)
                        |SET r = event.properties
                    """.trimMargin()
                    QueryEvents(query, it.value)
                }
    }

    override fun deleteRelationshipEvents(events: List<StreamsTransactionEvent>): List<QueryEvents> {
        if (events.isNullOrEmpty()) {
            return emptyList()
        }
        return events
                .filter { it.payload.type == EntityType.relationship && it.meta.operation == OperationType.deleted }
                .map {
                    val payload = it.payload as RelationshipPayload

                    if (payload.start.ids.isEmpty() || payload.end.ids.isEmpty()) {
                        throw RuntimeException("Node must have constraints defined in order to use the Schema Merge option")
                    }

                    val key = RelationshipSchemaMetadata(label = it.payload.label,
                            startLabels = payload.start.labels.orEmpty(),
                            endLabels = payload.end.labels.orEmpty(),
                            startKeys = payload.start.ids.keys,
                            endKeys = payload.end.ids.keys)
                    val value = mapOf("start" to payload.start.ids, "end" to payload.end.ids)

                    key to value
                }
                .groupBy { it.first }
                .mapValues { it.value.map { it.second } }
                .map {
                    val label = it.key.label
                    val startLabels = it.key.startLabels.joinToString(backtickSeparator)
                    val endLabels = it.key.endLabels.joinToString(backtickSeparator)
                    val startNodeKeys = it.key.startKeys.map { "`${it}`: event.start.`${it}`" }
                            .joinToString(keySeparator)
                    val endNodeKeys = it.key.endKeys.map { "`${it}`: event.end.`${it}`" }
                            .joinToString(keySeparator)
                    val query = """
                        |${StreamsUtils.UNWIND}
                        |MATCH (start:`$startLabels`{$startNodeKeys})
                        |MATCH (end:`$endLabels`{$endNodeKeys})
                        |MATCH (start)-[r:`$label`]->(end)
                        |DELETE r
                    """.trimMargin()
                    QueryEvents(query, it.value)
                }
    }

    override fun deleteNodeEvents(events: List<StreamsTransactionEvent>): List<QueryEvents> {
        if (events.isNullOrEmpty()) {
            return emptyList()
        }
        return events
                .filter { it.payload.type == EntityType.node && it.meta.operation == OperationType.deleted }
                .map {
                    val changeEvtBefore = it.payload.before as NodeChange
                    val constraints = getConstraints(it) { it.type == ConstraintType.NODE_KEY || it.type == ConstraintType.UNIQUENESS }

                    constraints to mapOf("properties" to changeEvtBefore.properties)
                }
                .groupBy { it.first }
                .map {
                    val nodeKeys = it.key
                            .flatMap { it.properties.map { "`$it`: event.properties.`$it`" } }
                            .joinToString(keySeparator)
                    val nodeLabels = it.key.map { it.label }.joinToString(backtickSeparator)
                    val query = """
                        |${StreamsUtils.UNWIND}
                        |MATCH (n:`$nodeLabels`{$nodeKeys})
                        |DETACH DELETE n
                    """.trimMargin()
                    QueryEvents(query, it.value.map { it.second })
                }
    }

    override fun mergeNodeEvents(events: List<StreamsTransactionEvent>): List<QueryEvents> {
        if (events.isNullOrEmpty()) {
            return emptyList()
        }
        return events
                .filter { it.payload.type == EntityType.node && it.meta.operation != OperationType.deleted }
                .map {
                    val changeEvtAfter = it.payload.after as NodeChange
                    val labelsAfter = changeEvtAfter.labels ?: emptyList()
                    val labelsBefore = if (it.payload.before != null) {
                        val changeEvtBefore = it.payload.before as NodeChange
                        changeEvtBefore.labels ?: emptyList()
                    } else {
                        emptyList()
                    }

                    val constraints = getConstraints(it) { it.type == ConstraintType.NODE_KEY || it.type == ConstraintType.UNIQUENESS }

                    val filterLabels: (String) -> Boolean = { label -> !constraints.any { constraint -> constraint.label == label } }

                    val labelsToAdd = (labelsAfter - labelsBefore)
                            .filter { filterLabels(it) }
                            .toSet()
                    val labelsToDelete = (labelsBefore - labelsAfter)
                            .filter { filterLabels(it) }
                            .toSet()
                    val keys = (changeEvtAfter.properties?.keys ?: emptySet())
                            .filter { key -> constraints.any { constraint -> constraint.properties.contains(key) } }
                            .toSet()

                    val key = NodeSchemaMetadata(constraints = constraints,
                            labelsToAdd = labelsToAdd, labelsToDelete = labelsToDelete,
                            keys = keys)
                    val value = mapOf("properties" to changeEvtAfter.properties)
                    key to value
                }
                .groupBy { it.first }
                .map { map ->
                    val setLabels = if (map.key.labelsToAdd.isNotEmpty()) {
                        "SET n:`${map.key.labelsToAdd.joinToString(backtickSeparator)}`"
                    } else {
                        ""
                    }
                    val removeLabels = if (map.key.labelsToDelete.isNotEmpty()) {
                        "REMOVE n:`${map.key.labelsToDelete.joinToString(backtickSeparator)}`"
                    } else {
                        ""
                    }
                    val nodeKeys = map.key.keys.map { "`$it`: event.properties.`$it`" }
                            .joinToString(keySeparator)
                    val nodeLabels = map.key.constraints.map { it.label }.joinToString(backtickSeparator)
                    val query = """
                        |${StreamsUtils.UNWIND}
                        |MERGE (n:`$nodeLabels`{$nodeKeys})
                        |SET n = event.properties
                        |$setLabels
                        |$removeLabels
                    """.trimMargin()
                    QueryEvents(query, map.value.map { it.second })
                }
    }

    private fun getConstraints(event: StreamsTransactionEvent, filter: (Constraint) -> Boolean): List<Constraint> {
        val constraints = event.schema.constraints.filter { filter(it) }

        if (constraints.isEmpty()) {
            throw RuntimeException("Node must have constraints defined in order to use the Schema Merge option")
        }
        return constraints
    }

}

data class NodeMergeMetadata(val labelsToAdd: Set<String>,
                             val labelsToDelete: Set<String>)

class MergeCDCQueryStrategy: CDCQueryStrategy {

    override fun mergeRelationshipEvents(events: List<StreamsTransactionEvent>): List<QueryEvents> {
        if (events.isNullOrEmpty()) {
            return emptyList()
        }
        return events
                .filter { it.payload.type == EntityType.relationship && it.meta.operation != OperationType.deleted }
                .map {
                    val payload = it.payload as RelationshipPayload
                    val changeEvt = when (it.meta.operation) {
                        OperationType.deleted -> {
                            it.payload.before as RelationshipChange
                        }
                        else -> it.payload.after as RelationshipChange
                    }
                    payload.label to mapOf("id" to payload.id,
                            "start" to payload.start.id, "end" to payload.end.id, "properties" to changeEvt.properties)
                }
                .groupBy { it.first }
                .map {
                    val query = """
                        |${StreamsUtils.UNWIND}
                        |MERGE (start:StreamsEvent{streams_id: event.start})
                        |MERGE (end:StreamsEvent{streams_id: event.end})
                        |MERGE (start)-[r:`${it.key}`{streams_id: event.id}]->(end)
                        |SET r = event.properties
                        |SET r.streams_id = event.id
                    """.trimMargin()
                    QueryEvents(query, it.value.map { it.second })
                }
    }

    override fun deleteRelationshipEvents(events: List<StreamsTransactionEvent>): List<QueryEvents> {
        if (events.isNullOrEmpty()) {
            return emptyList()
        }
        return events
                .filter { it.payload.type == EntityType.relationship && it.meta.operation == OperationType.deleted }
                .map {
                    val payload = it.payload as RelationshipPayload
                    payload.label to mapOf("id" to it.payload.id)
                }
                .groupBy { it.first }
                .map {
                    val query = "${StreamsUtils.UNWIND} MATCH ()-[r:`${it.key}`{streams_id: event.id}]-() DELETE r"
                    QueryEvents(query, it.value.map { it.second })
                }
    }

    override fun deleteNodeEvents(events: List<StreamsTransactionEvent>): List<QueryEvents> {
        if (events.isNullOrEmpty()) {
            return emptyList()
        }
        val data = events
                .filter { it.payload.type == EntityType.node && it.meta.operation == OperationType.deleted }
                .map { mapOf("id" to it.payload.id) }
        if (data.isNullOrEmpty()) {
            return emptyList()
        }
        val query = "${StreamsUtils.UNWIND} MATCH (n:StreamsEvent{streams_id: event.id}) DETACH DELETE n"
        return listOf(QueryEvents(query, data))
    }

    override fun mergeNodeEvents(events: List<StreamsTransactionEvent>): List<QueryEvents> {
        if (events.isNullOrEmpty()) {
            return emptyList()
        }
        return events
                .filter { it.payload.type == EntityType.node && it.meta.operation != OperationType.deleted }
                .map {
                    val changeEvtAfter = it.payload.after as NodeChange
                    val labelsAfter = changeEvtAfter.labels ?: emptyList()
                    val labelsBefore = if (it.payload.before != null) {
                        val changeEvtBefore = it.payload.before as NodeChange
                        changeEvtBefore.labels ?: emptyList()
                    } else {
                        emptyList()
                    }
                    val labelsToAdd = (labelsAfter - labelsBefore).toSet()
                    val labelsToDelete = (labelsBefore - labelsAfter).toSet()
                    NodeMergeMetadata(labelsToAdd = labelsToAdd, labelsToDelete = labelsToDelete) to mapOf("id" to it.payload.id, "properties" to changeEvtAfter.properties)
                }
                .groupBy { it.first }
                .map {
                    val setLabels = if (it.key.labelsToAdd.isNotEmpty()) {
                        "SET n:`${it.key.labelsToAdd.joinToString(backtickSeparator)}`"
                    } else {
                        ""
                    }
                    val removeLabels = if (it.key.labelsToDelete.isNotEmpty()) {
                        "REMOVE n:`${it.key.labelsToDelete.joinToString(backtickSeparator)}`"
                    } else {
                        ""
                    }
                    val query = """
                        |${StreamsUtils.UNWIND}
                        |MERGE (n:StreamsEvent{streams_id: event.id})
                        |SET n = event.properties
                        |SET n.streams_id = event.id
                        |$setLabels
                        |$removeLabels
                    """.trimMargin()
                    QueryEvents(query, it.value.map { it.second })
                }
    }

}