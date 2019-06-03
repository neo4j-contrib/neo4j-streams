package streams.service.sink.strategy

import streams.events.*
import streams.extensions.quote
import streams.serialization.JSONUtils
import streams.utils.IngestionUtils.getLabelsAsString
import streams.utils.StreamsUtils

data class SourceIdIngestionStrategyConfig(val labelName: String = "SourceEvent", val idName: String = "sourceId")

class SourceIdIngestionStrategy(config: SourceIdIngestionStrategyConfig = SourceIdIngestionStrategyConfig()): IngestionStrategy {

    private val quotedLabelName = config.labelName.quote()
    private val quotedIdName = config.idName.quote()

    override fun mergeRelationshipEvents(events: Collection<Any>): List<QueryEvents> {
        if (events.isNullOrEmpty()) {
            return emptyList()
        }
        return events
                .mapNotNull {
                    val data = JSONUtils.asStreamsTransactionEvent(it)
                    if (data.payload.type == EntityType.relationship && data.meta.operation != OperationType.deleted) {
                        val payload = data.payload as RelationshipPayload
                        val changeEvt = when (data.meta.operation) {
                            OperationType.deleted -> {
                                data.payload.before as RelationshipChange
                            }
                            else -> data.payload.after as RelationshipChange
                        }
                        payload.label to mapOf("id" to payload.id,
                                "start" to payload.start.id, "end" to payload.end.id, "properties" to changeEvt.properties)
                    } else {
                        null
                    }
                }
                .groupBy { it.first }
                .map {
                    val query = """
                        |${StreamsUtils.UNWIND}
                        |MERGE (start:$quotedLabelName{$quotedIdName: event.start})
                        |MERGE (end:$quotedLabelName{$quotedIdName: event.end})
                        |MERGE (start)-[r:${it.key.quote()}{$quotedIdName: event.id}]->(end)
                        |SET r = event.properties
                        |SET r.$quotedIdName = event.id
                    """.trimMargin()
                    QueryEvents(query, it.value.map { it.second })
                }
    }

    override fun deleteRelationshipEvents(events: Collection<Any>): List<QueryEvents> {
        if (events.isNullOrEmpty()) {
            return emptyList()
        }
        return events
                .mapNotNull {
                    val data = JSONUtils.asStreamsTransactionEvent(it)
                    if (data.payload.type == EntityType.relationship && data.meta.operation == OperationType.deleted) {
                        val payload = data.payload as RelationshipPayload
                        payload.label to mapOf("id" to data.payload.id)
                    } else {
                        null
                    }
                }
                .groupBy { it.first }
                .map {
                    val query = "${StreamsUtils.UNWIND} MATCH ()-[r:${it.key.quote()}{$quotedIdName: event.id}]-() DELETE r"
                    QueryEvents(query, it.value.map { it.second })
                }
    }

    override fun deleteNodeEvents(events: Collection<Any>): List<QueryEvents> {
        if (events.isNullOrEmpty()) {
            return emptyList()
        }
        val data = events
                .mapNotNull {
                    val data = JSONUtils.asStreamsTransactionEvent(it)
                    if (data.payload.type == EntityType.node && data.meta.operation == OperationType.deleted) {
                        mapOf("id" to data.payload.id)
                    } else {
                        null
                    }
                }
        if (data.isNullOrEmpty()) {
            return emptyList()
        }
        val query = "${StreamsUtils.UNWIND} MATCH (n:$quotedLabelName{$quotedIdName: event.id}) DETACH DELETE n"
        return listOf(QueryEvents(query, data))
    }

    override fun mergeNodeEvents(events: Collection<Any>): List<QueryEvents> {
        if (events.isNullOrEmpty()) {
            return emptyList()
        }
        return events
                .mapNotNull {
                    val data = JSONUtils.asStreamsTransactionEvent(it)
                    if (data.payload.type == EntityType.node && data.meta.operation != OperationType.deleted) {
                        val changeEvtAfter = data.payload.after as NodeChange
                        val labelsAfter = changeEvtAfter.labels ?: emptyList()
                        val labelsBefore = if (data.payload.before != null) {
                            val changeEvtBefore = data.payload.before as NodeChange
                            changeEvtBefore.labels ?: emptyList()
                        } else {
                            emptyList()
                        }
                        val labelsToAdd = (labelsAfter - labelsBefore)
                                .toSet()
                        val labelsToDelete = (labelsBefore - labelsAfter)
                                .toSet()
                        NodeMergeMetadata(labelsToAdd = labelsToAdd, labelsToDelete = labelsToDelete) to mapOf("id" to data.payload.id, "properties" to changeEvtAfter.properties)
                    } else {
                        null
                    }
                }
                .groupBy { it.first }
                .map {
                    var query = """
                        |${StreamsUtils.UNWIND}
                        |MERGE (n:$quotedLabelName{$quotedIdName: event.id})
                        |SET n = event.properties
                        |SET n.$quotedIdName = event.id
                    """.trimMargin()
                    if (it.key.labelsToDelete.isNotEmpty()) {
                        query += "\nREMOVE n:${getLabelsAsString(it.key.labelsToDelete)}"
                    }
                    if (it.key.labelsToAdd.isNotEmpty()) {
                        query += "\nSET n:${getLabelsAsString(it.key.labelsToAdd)}"
                    }
                    QueryEvents(query, it.value.map { it.second })
                }
    }

}