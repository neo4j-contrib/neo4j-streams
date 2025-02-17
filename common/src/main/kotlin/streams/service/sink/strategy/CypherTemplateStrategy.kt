package streams.service.sink.strategy

import org.neo4j.caniuse.CanIUse.canIUse
import org.neo4j.caniuse.Cypher
import org.neo4j.caniuse.Neo4j
import streams.service.StreamsSinkEntity
import streams.utils.StreamsUtils

class CypherTemplateStrategy(neo4j: Neo4j, query: String) : IngestionStrategy {
    private val cypherPrefix = if (canIUse(Cypher.explicitCypher5Selection()).withNeo4j(neo4j)) "CYPHER 5 " else ""
    private val fullQuery = "${cypherPrefix}${StreamsUtils.UNWIND} $query"

    override fun mergeNodeEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
        return listOf(QueryEvents(fullQuery, events.mapNotNull { it.value as? Map<String, Any> }))
    }

    override fun deleteNodeEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> = emptyList()

    override fun mergeRelationshipEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> = emptyList()

    override fun deleteRelationshipEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> = emptyList()

}