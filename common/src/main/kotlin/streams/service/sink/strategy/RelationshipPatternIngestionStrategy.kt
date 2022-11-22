package streams.service.sink.strategy

import streams.extensions.flatten
import streams.utils.JSONUtils
import streams.service.StreamsSinkEntity
import streams.utils.IngestionUtils.containsProp
import streams.utils.IngestionUtils.getLabelsAsString
import streams.utils.IngestionUtils.getNodeMergeKeys
import streams.utils.StreamsUtils

class RelationshipPatternIngestionStrategy(private val relationshipPatternConfiguration: RelationshipPatternConfiguration): IngestionStrategy {

    private val mergeRelationshipTemplate: String = """
                |${StreamsUtils.UNWIND}
                |MERGE (start${getLabelsAsString(relationshipPatternConfiguration.start.labels)}{${
                    getNodeMergeKeys("start.keys", relationshipPatternConfiguration.start.keys)
                }})
                |SET start ${if (relationshipPatternConfiguration.mergeProperties) "+" else ""}= event.start.properties
                |SET start += event.start.keys
                |MERGE (end${getLabelsAsString(relationshipPatternConfiguration.end.labels)}{${
                    getNodeMergeKeys("end.keys", relationshipPatternConfiguration.end.keys)
                }})
                |SET end ${if (relationshipPatternConfiguration.mergeProperties) "+" else ""}= event.end.properties
                |SET end += event.end.keys
                |MERGE (start)-[r:${relationshipPatternConfiguration.relType}]->(end)
                |SET r ${if (relationshipPatternConfiguration.mergeProperties) "+" else ""}= event.properties
            """.trimMargin()

    private val deleteRelationshipTemplate: String = """
                |${StreamsUtils.UNWIND}
                |MATCH (start${getLabelsAsString(relationshipPatternConfiguration.start.labels)}{${
                    getNodeMergeKeys("start.keys", relationshipPatternConfiguration.start.keys)
                }})
                |MATCH (end${getLabelsAsString(relationshipPatternConfiguration.end.labels)}{${
                    getNodeMergeKeys("end.keys", relationshipPatternConfiguration.end.keys)
                }})
                |MATCH (start)-[r:${relationshipPatternConfiguration.relType}]->(end)
                |DELETE r
            """.trimMargin()

    override fun mergeNodeEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
        return emptyList()
    }

    override fun deleteNodeEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
        return emptyList()
    }

    override fun mergeRelationshipEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
        val data = events
                .mapNotNull { if (it.value != null) JSONUtils.asMap(it.value) else null }
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
        return if (data.isEmpty()) {
            emptyList()
        } else {
            listOf(QueryEvents(mergeRelationshipTemplate, data))
        }
    }

    private fun isRelationshipProperty(propertyName: String): Boolean {
        return (!relationshipPatternConfiguration.start.keys.contains(propertyName)
                && !relationshipPatternConfiguration.start.properties.contains(propertyName)
                && !relationshipPatternConfiguration.end.keys.contains(propertyName)
                && !relationshipPatternConfiguration.end.properties.contains(propertyName))
    }

    override fun deleteRelationshipEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
        val data = events
                .filter { it.value == null && it.key != null }
                .mapNotNull { if (it.key != null) JSONUtils.asMap(it.key) else null }
                .mapNotNull { props ->
                    val properties = props.flatten()
                    val containsKeys = relationshipPatternConfiguration.start.keys.all { properties.containsKey(it) }
                            && relationshipPatternConfiguration.end.keys.all { properties.containsKey(it) }
                    if (containsKeys) {
                        val startConf = relationshipPatternConfiguration.start
                        val endConf = relationshipPatternConfiguration.end

                        val start = NodePatternIngestionStrategy.toData(startConf, props)
                        val end = NodePatternIngestionStrategy.toData(endConf, props)

                        mapOf("start" to start, "end" to end)
                    } else {
                        null
                    }
                }
        return if (data.isEmpty()) {
            emptyList()
        } else {
            listOf(QueryEvents(deleteRelationshipTemplate, data))
        }
    }

}