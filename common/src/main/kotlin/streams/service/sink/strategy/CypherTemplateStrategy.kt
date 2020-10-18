package streams.service.sink.strategy

import streams.service.StreamsSinkEntity
import streams.utils.StreamsUtils

class CypherTemplateStrategy(query: String): IngestionStrategy {
    private val fullQuery = "${StreamsUtils.UNWIND} $query"
    override fun mergeNodeEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
        return listOf(QueryEvents(fullQuery, events.mapNotNull { it.value as? Map<String, Any> }))
    }

    override fun deleteNodeEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> = emptyList()

    override fun mergeRelationshipEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> = emptyList()

    override fun deleteRelationshipEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> = emptyList()

}