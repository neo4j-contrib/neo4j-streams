package streams

import org.apache.kafka.common.internals.Topic
import streams.events.EntityType

object RoutingConfigurationConstants {
    const val NODE_ROUTING_KEY_PREFIX: String = "kafka.routing.nodes."
    const val REL_ROUTING_KEY_PREFIX: String = "kafka.routing.relationships."
}

private val PATTERN_REG: Regex = "^(\\w+\\s*(?::\\s*(?:[\\w|\\*]+)\\s*)*)\\s*(?:\\{\\s*(-?[\\w|\\*]+\\s*(?:,\\s*-?[\\w|\\*]+\\s*)*)\\})?\$".toRegex()
private val PATTERN_COLON_REG = "\\s*:\\s*".toRegex()
private val PATTERN_COMMA = "\\s*,\\s*".toRegex()
private const val PATTERN_WILDCARD = "*"
private const val PATTERN_PROP_MINUS = '-'
private const val PATTERN_SPLIT = ";"

data class RoutingProperties(val all: Boolean,
                             val include: List<String>,
                             val exclude: List<String>) {
    companion object {
        fun from(matcher: MatchResult): RoutingProperties {
            val props = matcher.groupValues[2].trim().let { if (it.isEmpty()) emptyList() else it.trim().split(PATTERN_COMMA) }
            val include = if (props.isEmpty()) {
                emptyList()
            } else {
                props.filter { it != PATTERN_WILDCARD && !it.startsWith(PATTERN_PROP_MINUS) }
            }
            val exclude = if (props.isEmpty()) {
                emptyList()
            } else {
                props.filter { it != PATTERN_WILDCARD && it.startsWith(PATTERN_PROP_MINUS) }.map { it.substring(1) }
            }
            val all = props.isEmpty() || props.contains(PATTERN_WILDCARD)
            return RoutingProperties(all = all, include = include, exclude = exclude)
        }
    }
}

abstract class RoutingConfiguration {
    abstract val topic: String
    abstract val all: Boolean
    abstract val include: List<String>
    abstract val exclude: List<String>
}

data class NodeRoutingConfiguration(val labels: List<String> = emptyList(),
                                    override val topic: String = "neo4j",
                                    override val all: Boolean = true,
                                    override val include: List<String> = emptyList(),
                                    override val exclude: List<String> = emptyList()): RoutingConfiguration() {

    companion object {
        fun parse(topic: String, pattern: String): List<NodeRoutingConfiguration> {
            Topic.validate(topic)
            if (pattern == PATTERN_WILDCARD) {
                return listOf(NodeRoutingConfiguration(topic = topic))
            }
            return pattern.split(PATTERN_SPLIT).map {
                val matcher = PATTERN_REG.matchEntire(it)
                if (matcher == null) {
                    throw IllegalArgumentException("The pattern $pattern for topic $topic is invalid")
                } else {
                    val labels = matcher.groupValues[1].split(PATTERN_COLON_REG)
                    val properties = RoutingProperties.from(matcher)
                    NodeRoutingConfiguration(labels = labels, topic = topic, all = properties.all,
                            include = properties.include, exclude = properties.exclude)
                }
            }

        }
    }
}

data class RelationshipRoutingConfiguration(val name: String = "",
                                            override val topic: String = "neo4j",
                                            override val all: Boolean = true,
                                            override val include: List<String> = emptyList(),
                                            override val exclude: List<String> = emptyList()): RoutingConfiguration() {

    companion object {
        fun parse(topic: String, pattern: String): List<RelationshipRoutingConfiguration> {
            Topic.validate(topic)
            if (pattern == PATTERN_WILDCARD) {
                return listOf(RelationshipRoutingConfiguration(topic = topic))
            }
            return pattern.split(PATTERN_SPLIT).map {
                val matcher = PATTERN_REG.matchEntire(it)
                if (matcher == null) {
                    throw IllegalArgumentException("The pattern $pattern for topic $topic is invalid")
                } else {
                    val labels = matcher.groupValues[1].split(PATTERN_COLON_REG)
                    if (labels.size > 1) {
                        throw IllegalArgumentException("The pattern $pattern for topic $topic is invalid")
                    }
                    val properties = RoutingProperties.from(matcher)
                    RelationshipRoutingConfiguration(name = labels.first(), topic = topic, all = properties.all,
                            include = properties.include, exclude = properties.exclude)
                }
            }
        }
    }
}

object RoutingConfigurationFactory {
    fun getRoutingConfiguration(topic: String, line: String, entityType: EntityType): List<RoutingConfiguration> {
        return when (entityType) {
            EntityType.node -> NodeRoutingConfiguration.parse(topic, line)
            EntityType.relationship -> RelationshipRoutingConfiguration.parse(topic, line)
        }
    }
}
