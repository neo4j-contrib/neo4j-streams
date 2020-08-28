package streams

import org.apache.commons.lang3.StringUtils
import streams.config.StreamsConfig
import streams.events.EntityType


private inline fun <reified T> filterMap(config: Map<String, String>, routingPrefix: String, dbName: String = ""): List<T> {
    val entityType = when (T::class) {
        NodeRoutingConfiguration::class -> EntityType.node
        RelationshipRoutingConfiguration::class -> EntityType.relationship
        else -> throw IllegalArgumentException("The class must be an instance of RoutingConfiguration")
    }
    return config
            .filterKeys {
                if (it.contains(StreamsRoutingConfigurationConstants.FROM)) {
                    val topicDbName = it.replace(routingPrefix, StringUtils.EMPTY)
                            .split(StreamsRoutingConfigurationConstants.FROM)[1]
                    it.startsWith(routingPrefix) && topicDbName == dbName // for `from.<db>` we compare the routing prefix and the db name
                } else {
                    dbName == "" && it.startsWith(routingPrefix) // for the default db we only filter by routingPrefix
                }
            }
            .flatMap {
                val topic = it.key.replace(routingPrefix, StringUtils.EMPTY)
                        .split(StreamsRoutingConfigurationConstants.FROM)[0]
                RoutingConfigurationFactory
                    .getRoutingConfiguration(topic, it.value, entityType) as List<T>
            }
}

private object StreamsRoutingConfigurationConstants {
    const val NODE_ROUTING_KEY_PREFIX: String = "streams.source.topic.nodes."
    const val REL_ROUTING_KEY_PREFIX: String = "streams.source.topic.relationships."
    const val SCHEMA_POLLING_INTERVAL = "streams.source.schema.polling.interval"
    const val FROM = ".from."
}

data class StreamsEventRouterConfiguration(val enabled: Boolean = StreamsConfig.SOURCE_ENABLED_VALUE,
                                           val proceduresEnabled: Boolean = StreamsConfig.PROCEDURES_ENABLED_VALUE,
                                           val nodeRouting: List<NodeRoutingConfiguration> = listOf(NodeRoutingConfiguration()),
                                           val relRouting: List<RelationshipRoutingConfiguration> = listOf(RelationshipRoutingConfiguration()),
                                           val schemaPollingInterval: Long = 300000) {

    fun allTopics(): List<String> {
        val nodeTopics = nodeRouting.map { it.topic }
        val relTopics = relRouting.map { it.topic }
        return nodeTopics + relTopics
    }

    companion object {

        fun from(streamsConfig: StreamsConfig, dbName: String): StreamsEventRouterConfiguration {
            val isDefaultDb = streamsConfig.isDefaultDb(dbName)

            var nodeRouting = filterMap<NodeRoutingConfiguration>(config = streamsConfig.config,
                    routingPrefix = StreamsRoutingConfigurationConstants.NODE_ROUTING_KEY_PREFIX,
                    dbName = dbName)
            var relRouting = filterMap<RelationshipRoutingConfiguration>(config = streamsConfig.config,
                    routingPrefix = StreamsRoutingConfigurationConstants.REL_ROUTING_KEY_PREFIX,
                    dbName = dbName)

            if (isDefaultDb) {
                nodeRouting += filterMap<NodeRoutingConfiguration>(config = streamsConfig.config,
                        routingPrefix = StreamsRoutingConfigurationConstants.NODE_ROUTING_KEY_PREFIX)
                relRouting += filterMap<RelationshipRoutingConfiguration>(config = streamsConfig.config,
                        routingPrefix = StreamsRoutingConfigurationConstants.REL_ROUTING_KEY_PREFIX)
            }

            val default = StreamsEventRouterConfiguration()
            return default.copy(
                    enabled = streamsConfig.isSourceEnabled(dbName),
                    proceduresEnabled = streamsConfig.hasProceduresEnabled(dbName),
                    nodeRouting = if (nodeRouting.isEmpty()) listOf(NodeRoutingConfiguration(topic = dbName)) else nodeRouting,
                    relRouting = if (relRouting.isEmpty()) listOf(RelationshipRoutingConfiguration(topic = dbName)) else relRouting,
                    schemaPollingInterval = streamsConfig.config
                            .getOrDefault(StreamsRoutingConfigurationConstants.SCHEMA_POLLING_INTERVAL, default.schemaPollingInterval).toString().toLong()
            )
        }

    }
}
