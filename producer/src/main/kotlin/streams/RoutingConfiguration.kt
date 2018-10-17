package streams

import org.apache.kafka.common.internals.Topic
import streams.events.*


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

private fun hasLabel(label: String, streamsEvent: StreamsEvent): Boolean {
    if (streamsEvent.payload.type == EntityType.relationship) {
        return false
    }
    val payload = when(streamsEvent.meta.operation) {
        OperationType.deleted -> streamsEvent.payload.before as NodeChange
        else -> streamsEvent.payload.after as NodeChange
    }
    return payload.labels.orEmpty().contains(label)
}

private fun isRelationshipType(name: String, streamsEvent: StreamsEvent): Boolean {
    if (streamsEvent.payload.type == EntityType.node) {
        return false
    }
    val relationshipChange = streamsEvent.payload as RelationshipPayload
    return relationshipChange.label == name
}

private fun filterProperties(recordChange: RecordChange?, routingConfiguration: RoutingConfiguration): Map<String, Any>? {
    if (recordChange == null) {
        return null
    }
    if (!routingConfiguration.all) {
        if (routingConfiguration.include.isNotEmpty()) {
            return recordChange.properties!!.filter { prop -> routingConfiguration.include.contains(prop.key) }
        }
        if (routingConfiguration.exclude.isNotEmpty()) {
            return recordChange.properties!!.filter { prop -> !routingConfiguration.exclude.contains(prop.key) }
        }

    }
    return recordChange.properties
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

        fun prepareEvent(streamsEvent: StreamsEvent, routingConf: List<NodeRoutingConfiguration>): Map<String, StreamsEvent> {
            return routingConf
                    .filter {
                        it.all || it.labels.any { hasLabel(it, streamsEvent) }
                    }
                    .map {
                        val nodePayload = streamsEvent.payload as NodePayload
                        val newRecordBefore: RecordChange? = if (nodePayload.before != null) {
                            val recordBefore = nodePayload.before as NodeChange
                            recordBefore.copy(properties = filterProperties(streamsEvent.payload.before, it),
                                    labels = recordBefore.labels)
                        } else {
                            null
                        }
                        val newRecordAfter: RecordChange? = if (nodePayload.after != null) {
                            val recordAfter = nodePayload.after as NodeChange
                            recordAfter.copy(properties = filterProperties(streamsEvent.payload.after!!, it),
                                    labels = recordAfter.labels)
                        } else {
                            null
                        }

                        val newNodePayload = nodePayload.copy(id = nodePayload.id,
                                before = newRecordBefore,
                                after = newRecordAfter)

                        val newStreamsEvent = streamsEvent.copy(schema = streamsEvent.schema,
                                meta = streamsEvent.meta,
                                payload = newNodePayload)

                        it.topic to newStreamsEvent
                    }
                    .associateBy({ it.first }, { it.second })
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

        fun prepareEvent(streamsEvent: StreamsEvent, routingConf: List<RelationshipRoutingConfiguration>): Map<String, StreamsEvent> {
            return routingConf
                    .filter {
                        it.all || isRelationshipType(it.name, streamsEvent)
                    }
                    .map {
                        val relationshipPayload = streamsEvent.payload as RelationshipPayload

                        val newRecordBefore: RecordChange? = if (relationshipPayload.before != null) {
                            val recordBefore = relationshipPayload.before as RelationshipChange
                            recordBefore.copy(properties = filterProperties(streamsEvent.payload.before, it))
                        } else {
                            null
                        }
                        val newRecordAfter: RecordChange? = if (relationshipPayload.after != null) {
                            val recordAfter = relationshipPayload.after as RelationshipChange
                            recordAfter.copy(properties = filterProperties(streamsEvent.payload.after, it))
                        } else {
                            null
                        }

                        val newRelationshipPayload = relationshipPayload.copy(id = relationshipPayload.id,
                                before = newRecordBefore,
                                after = newRecordAfter,
                                label = relationshipPayload.label)

                        val newStreamsEvent = streamsEvent.copy(schema = streamsEvent.schema,
                                meta = streamsEvent.meta,
                                payload = newRelationshipPayload)

                        it.topic to newStreamsEvent
                    }
                    .associateBy({ it.first }, { it.second })
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
