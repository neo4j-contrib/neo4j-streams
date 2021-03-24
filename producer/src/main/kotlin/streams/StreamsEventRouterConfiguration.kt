package streams

import org.apache.commons.lang3.StringUtils
import org.neo4j.logging.Log
import streams.events.EntityType


enum class RelKeyStrategy { first, all }

private inline fun <reified T> filterMap(config: Map<String, String>, routingPrefix: String, routingSuffix: String? = null): List<T> {
    val entityType = when (T::class) {
        NodeRoutingConfiguration::class -> EntityType.node
        RelationshipRoutingConfiguration::class -> EntityType.relationship
        else -> throw IllegalArgumentException("The class must be an instance of RoutingConfiguration")
    }
    return config
            .filterKeys { it.startsWith(routingPrefix) && routingSuffix?.let { suffix -> !it.endsWith(suffix) } ?: true }
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
    const val KEY_STRATEGY_SUFFIX = "key_strategy"
}

data class StreamsEventRouterConfiguration(val enabled: Boolean = true,
                                           val proceduresEnabled: Boolean = true,
                                           val nodeRouting: List<NodeRoutingConfiguration> = listOf(NodeRoutingConfiguration()),
                                           val relRouting: List<RelationshipRoutingConfiguration> = listOf(RelationshipRoutingConfiguration()),
                                           val relKeyStrategies: List<RelKeyStrategyConfiguration> = listOf(RelKeyStrategyConfiguration()),
                                           val schemaPollingInterval: Long = 300000) {

    fun allTopics(): List<String> {
        val nodeTopics = nodeRouting.map { it.topic }
        val relTopics = relRouting.map { it.topic }
        return nodeTopics + relTopics
    }

    companion object {
        fun from(config: Map<String, String>, log: Log? = null): StreamsEventRouterConfiguration {
            val nodeRouting = filterMap<NodeRoutingConfiguration>(config = config,
                    routingPrefix = StreamsRoutingConfigurationConstants.NODE_ROUTING_KEY_PREFIX)

            val relRouting = filterMap<RelationshipRoutingConfiguration>(config = config,
                    routingPrefix = StreamsRoutingConfigurationConstants.REL_ROUTING_KEY_PREFIX,
                    routingSuffix = StreamsRoutingConfigurationConstants.KEY_STRATEGY_SUFFIX
            )

            val relKeyStrategies = config.filterKeys { it.startsWith(StreamsRoutingConfigurationConstants.REL_ROUTING_KEY_PREFIX)
                    && it.endsWith(StreamsRoutingConfigurationConstants.KEY_STRATEGY_SUFFIX)
            }.map {
                val topic = it.key.replace(StreamsRoutingConfigurationConstants.REL_ROUTING_KEY_PREFIX, StringUtils.EMPTY).split(".")[0]
                RelKeyStrategyConfiguration.parse(topic, it.value, log)
            }

            val default = StreamsEventRouterConfiguration()
            return default.copy(enabled = config.getOrDefault(StreamsRoutingConfigurationConstants.ENABLED, default.enabled).toString().toBoolean(),
                    proceduresEnabled = config.getOrDefault(StreamsRoutingConfigurationConstants.PROCEDURES_ENABLED, default.proceduresEnabled).toString().toBoolean(),
                    nodeRouting = if (nodeRouting.isEmpty()) default.nodeRouting else nodeRouting,
                    relRouting = if (relRouting.isEmpty()) default.relRouting else relRouting,
                    relKeyStrategies = if (relKeyStrategies.isEmpty()) default.relKeyStrategies else relKeyStrategies,
                    schemaPollingInterval = config.getOrDefault(StreamsRoutingConfigurationConstants.SCHEMA_POLLING_INTERVAL, default.schemaPollingInterval).toString().toLong())
        }
    }
}
