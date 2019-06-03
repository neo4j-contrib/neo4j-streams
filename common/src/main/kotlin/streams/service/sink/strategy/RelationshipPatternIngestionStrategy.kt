package streams.service.sink.strategy

import streams.extensions.flatten
import streams.utils.IngestionUtils.containsProp
import streams.utils.IngestionUtils.getLabelsAsString
import streams.utils.IngestionUtils.getNodeMergeKeys
import streams.utils.StreamsUtils

class RelationshipPatternIngestionStrategy(private val relationshipPatternConfiguration: RelationshipPatternConfiguration): IngestionStrategy {

    private val relationshipTemplate: String = """
                |${StreamsUtils.UNWIND}
                |MERGE (start:${getLabelsAsString(relationshipPatternConfiguration.start.labels)}{${
                    getNodeMergeKeys("start.keys", relationshipPatternConfiguration.start.keys)
                }})
                |SET start = event.start.properties
                |SET start += event.start.keys
                |MERGE (end:${getLabelsAsString(relationshipPatternConfiguration.end.labels)}{${
                    getNodeMergeKeys("end.keys", relationshipPatternConfiguration.end.keys)
                }})
                |SET end = event.end.properties
                |SET end += event.end.keys
                |MERGE (start)-[r:${relationshipPatternConfiguration.relType}]->(end)
                |SET r = event.properties
            """.trimMargin()

    private val needsToBeFlattened = relationshipPatternConfiguration.properties.isEmpty()
            || relationshipPatternConfiguration.properties.any { it.contains(".") }


    override fun mergeNodeEvents(events: Collection<Any>): List<QueryEvents> {
        return emptyList()
    }

    override fun deleteNodeEvents(events: Collection<Any>): List<QueryEvents> {
        return emptyList()
    }

    override fun mergeRelationshipEvents(events: Collection<Any>): List<QueryEvents> {
        if (events.isNullOrEmpty()) {
            return emptyList()
        }
        val data = (events as List<Map<String, Any?>>)
                .mapNotNull { props ->
                    val properties = props.flatten()
                    val containsKeys = relationshipPatternConfiguration.start.keys.all { properties.containsKey(it) }
                            && relationshipPatternConfiguration.end.keys.all { properties.containsKey(it) }
                    if (containsKeys) {
                        val filteredProperties = when (relationshipPatternConfiguration.type) {
                            PatternConfigurationType.ALL -> properties.filterKeys { isRelationshipProperty(it) }
                            PatternConfigurationType.EXCLUDE -> properties.filterKeys {
                                val containsProp = containsProp(it, relationshipPatternConfiguration.properties)
                                isRelationshipProperty(it) && !containsProp
                            }
                            PatternConfigurationType.INCLUDE -> properties.filterKeys {
                                val containsProp = containsProp(it, relationshipPatternConfiguration.properties)
                                isRelationshipProperty(it) && containsProp
                            }
                        }
                        val startConf = relationshipPatternConfiguration.start
                        val endConf = relationshipPatternConfiguration.end

                        val start = NodePatternIngestionStrategy.toData(startConf, props)
                        val end = NodePatternIngestionStrategy.toData(endConf, props)

                        mapOf("start" to start, "end" to end, "properties" to filteredProperties)
                    } else {
                        null
                    }
                }
        return listOf(QueryEvents(relationshipTemplate, data))
    }

    private fun isRelationshipProperty(propertyName: String): Boolean {
        return (!relationshipPatternConfiguration.start.keys.contains(propertyName)
                && !relationshipPatternConfiguration.start.properties.contains(propertyName)
                && !relationshipPatternConfiguration.end.keys.contains(propertyName)
                && !relationshipPatternConfiguration.end.properties.contains(propertyName))
    }

    override fun deleteRelationshipEvents(events: Collection<Any>): List<QueryEvents> {
        return emptyList()
    }

}