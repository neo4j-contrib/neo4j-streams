package streams.service.sink.strategy

import org.neo4j.caniuse.CanIUse.canIUse
import org.neo4j.caniuse.Cypher
import org.neo4j.caniuse.Neo4j
import streams.extensions.flatten
import streams.utils.JSONUtils
import streams.service.StreamsSinkEntity
import streams.utils.IngestionUtils.containsProp
import streams.utils.IngestionUtils.getLabelsAsString
import streams.utils.IngestionUtils.getNodeMergeKeys
import streams.utils.StreamsUtils

class NodePatternIngestionStrategy(neo4j: Neo4j, private val nodePatternConfiguration: NodePatternConfiguration): IngestionStrategy {
    private val cypherPrefix = if (canIUse(Cypher.explicitCypher5Selection()).withNeo4j(neo4j)) "CYPHER 5 " else ""

    private val mergeNodeTemplate: String = """
                |${cypherPrefix}${StreamsUtils.UNWIND}
                |MERGE (n${getLabelsAsString(nodePatternConfiguration.labels)}{${
                    getNodeMergeKeys("keys", nodePatternConfiguration.keys)
                }})
                |SET n ${if (nodePatternConfiguration.mergeProperties) "+" else ""}= event.properties
                |SET n += event.keys
            """.trimMargin()

    private val deleteNodeTemplate: String = """
                |${cypherPrefix}${StreamsUtils.UNWIND}
                |MATCH (n${getLabelsAsString(nodePatternConfiguration.labels)}{${
                    getNodeMergeKeys("keys", nodePatternConfiguration.keys)
                }})
                |DETACH DELETE n
            """.trimMargin()

    override fun mergeNodeEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
        val data = events
                .mapNotNull { if (it.value != null) JSONUtils.asMap(it.value) else null }
                .mapNotNull { toData(nodePatternConfiguration, it) }
        return if (data.isEmpty()) {
            emptyList()
        } else {
            listOf(QueryEvents(mergeNodeTemplate, data))
        }
    }

    override fun deleteNodeEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
        val data = events
                .filter { it.value == null && it.key != null }
                .mapNotNull { if (it.key != null) JSONUtils.asMap(it.key) else null }
                .mapNotNull { toData(nodePatternConfiguration, it, false) }
        return if (data.isEmpty()) {
            emptyList()
        } else {
            listOf(QueryEvents(deleteNodeTemplate, data))
        }
    }

    override fun mergeRelationshipEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
        return emptyList()
    }

    override fun deleteRelationshipEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
        return emptyList()
    }

    companion object {
        fun toData(nodePatternConfiguration: NodePatternConfiguration, props: Map<String, Any?>, withProperties: Boolean = true): Map<String, Map<String, Any?>>? {
            val properties = props.flatten()
            val containsKeys = nodePatternConfiguration.keys.all { properties.containsKey(it) }
            return if (containsKeys) {
                val filteredProperties = when (nodePatternConfiguration.type) {
                    PatternConfigurationType.ALL -> properties.filterKeys { !nodePatternConfiguration.keys.contains(it) }
                    PatternConfigurationType.EXCLUDE -> properties.filterKeys { key ->
                        val containsProp = containsProp(key, nodePatternConfiguration.properties)
                        !nodePatternConfiguration.keys.contains(key) && !containsProp
                    }
                    PatternConfigurationType.INCLUDE -> properties.filterKeys { key ->
                        val containsProp = containsProp(key, nodePatternConfiguration.properties)
                        !nodePatternConfiguration.keys.contains(key) && containsProp
                    }
                }
                if (withProperties) {
                    mapOf("keys" to properties.filterKeys { nodePatternConfiguration.keys.contains(it) },
                            "properties" to filteredProperties)
                } else {
                    mapOf("keys" to properties.filterKeys { nodePatternConfiguration.keys.contains(it) })
                }
            } else {
                null
            }
        }


    }

}