package streams

import org.apache.commons.lang3.StringUtils
import streams.events.EntityType


private inline fun <reified T> filterMap(config: Map<String, String>, routingPrefix: String): List<T> {
    val entityType = when (T::class) {
        NodeRoutingConfiguration::class -> EntityType.node
        RelationshipRoutingConfiguration::class -> EntityType.relationship
        else -> throw IllegalArgumentException("The class must be an instance of RoutingConfiguration")
    }
    return config
            .filterKeys { it.startsWith(routingPrefix) }
            .flatMap { RoutingConfigurationFactory
                    .getRoutingConfiguration(it.key.replace(routingPrefix, StringUtils.EMPTY) , it.value, entityType) as List<T>
            }
}

private object StreamsRoutingConfigurationConstants {
    const val NODE_ROUTING_KEY_PREFIX: String = "streams.source.topic.nodes."
    const val REL_ROUTING_KEY_PREFIX: String = "streams.source.topic.relationships."
    const val ENABLED = "streams.source.enabled"
    const val SCHEMA_POLLING_INTERVAL = "streams.source.schema.polling.interval"
    const val PROCEDURES_ENABLED = "streams.procedures.enabled"
}

data class StreamsEventRouterConfiguration(val enabled: Boolean = true,
                                           val proceduresEnabled: Boolean = true,
                                           val nodeRouting: List<NodeRoutingConfiguration> = listOf(NodeRoutingConfiguration()),
                                           val relRouting: List<RelationshipRoutingConfiguration> = listOf(RelationshipRoutingConfiguration()),
                                           val schemaPollingInterval: Long = 300000) {

    fun allTopics(): List<String> {
        val nodeTopics = nodeRouting.map { it.topic }
        val relTopics = relRouting.map { it.topic }
        return nodeTopics + relTopics
    }

    companion object {
        fun from(config: Map<String, String>): StreamsEventRouterConfiguration {
            val nodeRouting = filterMap<NodeRoutingConfiguration>(config = config,
                    routingPrefix = StreamsRoutingConfigurationConstants.NODE_ROUTING_KEY_PREFIX)

            val relRouting = filterMap<RelationshipRoutingConfiguration>(config = config,
                    routingPrefix = StreamsRoutingConfigurationConstants.REL_ROUTING_KEY_PREFIX)

            val default = StreamsEventRouterConfiguration()
            return default.copy(enabled = config.getOrDefault(StreamsRoutingConfigurationConstants.ENABLED, default.enabled).toString().toBoolean(),
                    proceduresEnabled = config.getOrDefault(StreamsRoutingConfigurationConstants.PROCEDURES_ENABLED, default.proceduresEnabled).toString().toBoolean(),
                    nodeRouting = if (nodeRouting.isEmpty()) default.nodeRouting else nodeRouting,
                    relRouting = if (relRouting.isEmpty()) default.relRouting else relRouting,
                    schemaPollingInterval = config.getOrDefault(StreamsRoutingConfigurationConstants.SCHEMA_POLLING_INTERVAL, default.schemaPollingInterval).toString().toLong())
        }
    }
}
