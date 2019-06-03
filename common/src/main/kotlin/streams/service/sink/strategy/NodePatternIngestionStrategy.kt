package streams.service.sink.strategy

import streams.extensions.flatten
import streams.utils.IngestionUtils
import streams.utils.IngestionUtils.containsProp
import streams.utils.IngestionUtils.getLabelsAsString
import streams.utils.IngestionUtils.getNodeMergeKeys
import streams.utils.StreamsUtils

class NodePatternIngestionStrategy(private val nodePatternConfiguration: NodePatternConfiguration): IngestionStrategy {

    private val nodeTemplate: String = """
                |${StreamsUtils.UNWIND}
                |MERGE (n:${getLabelsAsString(nodePatternConfiguration.labels)}{${
                    getNodeMergeKeys("keys", nodePatternConfiguration.keys)
                }})
                |SET n = event.properties
                |SET n += event.keys
            """.trimMargin()

    override fun mergeNodeEvents(events: Collection<Any>): List<QueryEvents> {
        if (events.isNullOrEmpty()) {
            return emptyList()
        }

        val data = (events as List<Map<String, Any?>>)
                .mapNotNull {
                    toData(nodePatternConfiguration, it)
                }
        return listOf(QueryEvents(nodeTemplate, data))
    }

    override fun deleteNodeEvents(events: Collection<Any>): List<QueryEvents> {
        return emptyList()
    }

    override fun mergeRelationshipEvents(events: Collection<Any>): List<QueryEvents> {
        return emptyList()
    }

    override fun deleteRelationshipEvents(events: Collection<Any>): List<QueryEvents> {
        return emptyList()
    }

    companion object {
        fun toData(nodePatternConfiguration: NodePatternConfiguration, props: Map<String, Any?>): Map<String, Map<String, Any?>>? {
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
                mapOf("keys" to properties.filterKeys { nodePatternConfiguration.keys.contains(it) },
                        "properties" to filteredProperties)
            } else {
                null
            }
        }


    }

}