package streams

import streams.events.EntityType


private fun <T> filterMap(config: Map<String, String>, routingPrefix: String): List<T> {
    return config
            .filterKeys { it.startsWith(routingPrefix) }
            .flatMap { RoutingConfigurationFactory.getRoutingConfiguration(it.key.replace(routingPrefix, "") , it.value, EntityType.node) as List<T> }
}

private object StreamsRoutingConfigurationConstants {
    const val NODE_ROUTING_KEY_PREFIX: String = "streams.source.topic.nodes."
    const val REL_ROUTING_KEY_PREFIX: String = "streams.source.topic.relationships."
    const val ENABLED = "streams.produce.enabled"
}

data class StreamsEventRouterConfiguration(val enabled: Boolean = true,
                                           val nodeRouting: List<NodeRoutingConfiguration> = listOf(NodeRoutingConfiguration()),
                                           val relRouting: List<RelationshipRoutingConfiguration> = listOf(RelationshipRoutingConfiguration())) {
    companion object {
        fun from(config: Map<String, String>): StreamsEventRouterConfiguration {
            val nodeRouting = filterMap<NodeRoutingConfiguration>(config = config,
                    routingPrefix = StreamsRoutingConfigurationConstants.NODE_ROUTING_KEY_PREFIX)

            val relRouting = filterMap<RelationshipRoutingConfiguration>(config = config,
                    routingPrefix = StreamsRoutingConfigurationConstants.REL_ROUTING_KEY_PREFIX)

            val default = StreamsEventRouterConfiguration()
            return default.copy(enabled = config.getOrDefault(StreamsRoutingConfigurationConstants.ENABLED, default.enabled).toString().toBoolean(),
                    nodeRouting = if (nodeRouting.isEmpty()) default.nodeRouting else nodeRouting,
                    relRouting = if (relRouting.isEmpty()) default.relRouting else relRouting)
        }
    }
}
