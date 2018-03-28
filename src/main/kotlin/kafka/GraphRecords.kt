package kafka

import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node

enum class UpdateState { created, updated, deleted }
data class NodeRecord(val id : Long, val labels: List<String> = emptyList(), val data: Map<String, Any> = emptyMap(), val state: UpdateState) {
    constructor(n: Node, state: UpdateState) : this(n.id, n.labels.map { it.name() }, n.allProperties, state)

    companion object {
        fun apply(n: Node, pattern: NodePattern, state: UpdateState): NodeRecord {
            val props = if (pattern.all || pattern.include.isEmpty()) n.allProperties else n.getProperties(*pattern.include.toTypedArray())
            if (pattern.exclude.isNotEmpty()) props.keys.removeAll(pattern.exclude)
            return NodeRecord(n.id, n.labels.map { it.name() }, props, state)
        }
    }
}
data class RelationshipRecord(val id : Long, val type: String, val data: Map<String, Any>, val state: UpdateState)
