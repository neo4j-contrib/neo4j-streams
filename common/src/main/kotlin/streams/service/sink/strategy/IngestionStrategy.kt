package streams.service.sink.strategy

import streams.events.*
import streams.utils.Neo4jUtils
import streams.utils.StreamsUtils


data class QueryEvents(val query: String, val events: List<Map<String, Any?>>)

interface IngestionStrategy {
    fun mergeNodeEvents(events: Collection<Any>): List<QueryEvents>
    fun deleteNodeEvents(events: Collection<Any>): List<QueryEvents>
    fun mergeRelationshipEvents(events: Collection<Any>): List<QueryEvents>
    fun deleteRelationshipEvents(events: Collection<Any>): List<QueryEvents>
}

data class RelationshipSchemaMetadata(val label: String,
                                      val startLabels: List<String>,
                                      val endLabels: List<String>,
                                      val startKeys: Set<String>,
                                      val endKeys: Set<String>) {
    constructor(payload: RelationshipPayload) : this(label = payload.label,
            startLabels = payload.start.labels.orEmpty(),
            endLabels = payload.end.labels.orEmpty(),
            startKeys = payload.start.ids.keys,
            endKeys = payload.end.ids.keys)
}

data class NodeSchemaMetadata(val constraints: List<Constraint>,
                              val labelsToAdd: List<String>,
                              val labelsToDelete: List<String>,
                              val keys: Set<String>)



data class NodeMergeMetadata(val labelsToAdd: Set<String>,
                             val labelsToDelete: Set<String>)