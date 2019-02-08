package streams.service

import streams.events.*
import streams.utils.StreamsUtils

data class QueryEvents(val query: String, val events: List<Map<String, Any?>>)

interface CDCQueryStrategy {
    fun mergeNodeEvents(events: List<StreamsTransactionEvent>): List<QueryEvents>
    fun deleteNodeEvents(events: List<StreamsTransactionEvent>): List<QueryEvents>
    fun mergeRelationshipEvents(events: List<StreamsTransactionEvent>): List<QueryEvents>
    fun deleteRelationshipEvents(events: List<StreamsTransactionEvent>): List<QueryEvents>
}

class MergeCDCQueryStrategy: CDCQueryStrategy {

    private val separator = "`:`"

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
                    val start = payload.start.labels?.joinToString(separator) ?: ""
                    val end = payload.end.labels?.joinToString(separator) ?: ""
                    mapOf("start" to start, "label" to payload.label, "end" to end) to mapOf("id" to payload.id,
                            "start" to payload.start.id, "end" to payload.end.id, "properties" to changeEvt.properties)
                }
                .groupBy { it.first }
                .mapValues { it.value.map { it.second } }
                .filterValues { !it.isNullOrEmpty() }
                .map {
                    val query = """
                        ${StreamsUtils.UNWIND}
                        MERGE (start:StreamsEvent{streams_id: event.start})
                        MERGE (end:StreamsEvent{streams_id: event.end})
                        MERGE (start)-[r:`${it.key.getValue("label")}`{streams_id: event.id}]->(end)
                        SET r = event.properties
                        SET r.streams_id = event.id
                    """.trimIndent()
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
                    payload.label to mapOf("id" to it.payload.id)
                }
                .groupBy { it.first }
                .mapValues { it.value.map { it.second } }
                .filterValues { !it.isNullOrEmpty() }
                .map {
                    val query = "${StreamsUtils.UNWIND} MATCH ()-[r:`${it.key}`{streams_id: event.id}]-() DELETE r"
                    QueryEvents(query, it.value)
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
                    val toAdd = (labelsAfter - labelsBefore)
                            .joinToString(separator)
                    val toRemove = (labelsBefore - labelsAfter)
                            .joinToString(separator)
                    (toRemove to toAdd) to mapOf("id" to it.payload.id, "properties" to changeEvtAfter.properties)
                }
                .groupBy { it.first }
                .filterValues { !it.isNullOrEmpty() }
                .map {
                    val setLabels = if (it.key.second != "") {
                        "SET n:`${it.key.second}`"
                    } else {
                        ""
                    }
                    val removeLabels = if (it.key.first != "") {
                        "REMOVE n:`${it.key.first}`"
                    } else {
                        ""
                    }
                    val query = """
                        ${StreamsUtils.UNWIND}
                        MERGE (n:StreamsEvent{streams_id: event.id})
                        SET n = event.properties
                        SET n.streams_id = event.id
                        $setLabels
                        $removeLabels
                    """.trimIndent()
                    QueryEvents(query, it.value.map { it.second })
                }
    }

}