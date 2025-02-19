package streams.service.sink.strategy

import org.neo4j.caniuse.CanIUse.canIUse
import org.neo4j.caniuse.Cypher
import org.neo4j.caniuse.Neo4j
import streams.events.*
import streams.extensions.quote
import streams.service.StreamsSinkEntity
import streams.utils.IngestionUtils.getLabelsAsString
import streams.utils.SchemaUtils
import streams.utils.StreamsUtils

data class SourceIdIngestionStrategyConfig(val labelName: String = "SourceEvent", val idName: String = "sourceId") {
    companion object {
        val DEFAULT = SourceIdIngestionStrategyConfig()
    }
}

class SourceIdIngestionStrategy(
    neo4j: Neo4j,
    config: SourceIdIngestionStrategyConfig = SourceIdIngestionStrategyConfig()
) : IngestionStrategy {
    private val cypherPrefix = if (canIUse(Cypher.explicitCypher5Selection()).withNeo4j(neo4j)) "CYPHER 5 " else ""

    private val quotedLabelName = config.labelName.quote()
    private val quotedIdName = config.idName.quote()

    override fun mergeRelationshipEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
        return events
            .mapNotNull { SchemaUtils.toStreamsTransactionEvent(it) { it.payload.type == EntityType.relationship && it.meta.operation != OperationType.deleted } }
            .map { data ->
                val payload = data.payload as RelationshipPayload
                val changeEvt = when (data.meta.operation) {
                    OperationType.deleted -> {
                        data.payload.before as RelationshipChange
                    }

                    else -> data.payload.after as RelationshipChange
                }
                payload.label to mapOf(
                    "id" to payload.id,
                    "start" to payload.start.id, "end" to payload.end.id, "properties" to changeEvt.properties
                )
            }
            .groupBy({ it.first }, { it.second })
            .map {
                val query = """
                        |${cypherPrefix}${StreamsUtils.UNWIND}
                        |MERGE (start:$quotedLabelName{$quotedIdName: event.start})
                        |MERGE (end:$quotedLabelName{$quotedIdName: event.end})
                        |MERGE (start)-[r:${it.key.quote()}{$quotedIdName: event.id}]->(end)
                        |SET r = event.properties
                        |SET r.$quotedIdName = event.id
                    """.trimMargin()
                QueryEvents(query, it.value)
            }
    }

    override fun deleteRelationshipEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
        return events
            .mapNotNull { SchemaUtils.toStreamsTransactionEvent(it) { it.payload.type == EntityType.relationship && it.meta.operation == OperationType.deleted } }
            .map { data ->
                val payload = data.payload as RelationshipPayload
                payload.label to mapOf("id" to data.payload.id)
            }
            .groupBy({ it.first }, { it.second })
            .map {
                val query = "${cypherPrefix}${StreamsUtils.UNWIND} MATCH ()-[r:${it.key.quote()}{$quotedIdName: event.id}]-() DELETE r"
                QueryEvents(query, it.value)
            }
    }

    override fun deleteNodeEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
        val data = events
            .mapNotNull { SchemaUtils.toStreamsTransactionEvent(it) { it.payload.type == EntityType.node && it.meta.operation == OperationType.deleted } }
            .map { mapOf("id" to it.payload.id) }
        if (data.isNullOrEmpty()) {
            return emptyList()
        }
        val query = "${cypherPrefix}${StreamsUtils.UNWIND} MATCH (n:$quotedLabelName{$quotedIdName: event.id}) DETACH DELETE n"
        return listOf(QueryEvents(query, data))
    }

    override fun mergeNodeEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
        return events
            .mapNotNull { SchemaUtils.toStreamsTransactionEvent(it) { it.payload.type == EntityType.node && it.meta.operation != OperationType.deleted } }
            .map { data ->
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
                NodeMergeMetadata(
                    labelsToAdd = labelsToAdd,
                    labelsToDelete = labelsToDelete
                ) to mapOf("id" to data.payload.id, "properties" to changeEvtAfter.properties)
            }
            .groupBy({ it.first }, { it.second })
            .map {
                var query = """
                        |${cypherPrefix}${StreamsUtils.UNWIND}
                        |MERGE (n:$quotedLabelName{$quotedIdName: event.id})
                        |SET n = event.properties
                        |SET n.$quotedIdName = event.id
                    """.trimMargin()
                if (it.key.labelsToDelete.isNotEmpty()) {
                    query += "\nREMOVE n${getLabelsAsString(it.key.labelsToDelete)}"
                }
                if (it.key.labelsToAdd.isNotEmpty()) {
                    query += "\nSET n${getLabelsAsString(it.key.labelsToAdd)}"
                }
                QueryEvents(query, it.value)
            }
    }

}