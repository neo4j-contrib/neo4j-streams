package streams.events

import org.neo4j.graphdb.Node
import org.neo4j.graphdb.event.LabelEntry
import org.neo4j.graphdb.event.PropertyEntry

data class PreviousTransactionData(val nodeProperties: Map<Long, Map<String, Any>>,
                                   val nodeLabels: Map<Long, List<String>>,
                                   val updatedPayloads: List<NodePayload> = emptyList(),
                                   val createdPayload: List<NodePayload>,
                                   val deletedPayload: List<NodePayload>)

/**
 * Build a data class containing the previous (before) state of the nodes/relationships
 */
class PreviousTransactionDataBuilder(){

    private var nodeProperties : Map<Long,Map<String,Any>> = emptyMap()
    private var nodeLabels: Map<Long, List<String>> = emptyMap()
    private var updatedNodes : Set<Node> = emptySet()
    private var createdPayload: List<NodePayload> = emptyList()
    private var deletedPayload: List<NodePayload> = emptyList()

    fun build() : PreviousTransactionData{

        val createdIds = createdPayload.map { it.id }

        val updatedPayloads = updatedNodes
                .filter { ! createdIds.contains(it.id) }
                .map { it ->
            val labelsBefore = nodeLabels.getOrDefault(it.id, listOf())
            val propsBefore = nodeProperties.getOrDefault(it.id, mapOf())

            val beforeNode = NodeChangeBuilder()
                    .withLabels(labelsBefore)
                    .withProperites(propsBefore)
                    .build()

            val labelsAfter = it.labels.map { it.name() }

            val afterNode = NodeChangeBuilder()
                    .withLabels(labelsAfter)
                    .withProperites(it.allProperties)
                    .build()

            val payload = NodePayloadBuilder()
                    .withId(it.id)
                    .withBefore(beforeNode)
                    .withAfter(afterNode)
                    .build()

            payload
        }


        return PreviousTransactionData(nodeProperties, nodeLabels , updatedPayloads,createdPayload, deletedPayload)
    }

    fun withLabels(assignedLabels: Iterable<LabelEntry>, removedLabels: Iterable<LabelEntry>): PreviousTransactionDataBuilder {
        val assignedPreviousLabels = assignedLabels
                .map { labelEntry -> Pair(labelEntry.node().id, labelEntry.node().labels.filter { it != labelEntry.label() }.map { it -> it.name() }.toList()) } // [ (nodeId, [label]) ]
                .groupBy({it.first},{it.second}) // { nodeId -> [ [label] ] }
                .mapValues { it -> it.value.flatten() } // { nodeId -> [label] }

        val removedPreviousLabels = removedLabels
                .map { labelEntry -> Pair(labelEntry.node().id, labelEntry.node().labels.map { it -> it.name() }.toList().plus(labelEntry.label().name())) } // [ (nodeId, [label]) ]
                .groupBy({it.first},{it.second}) // { nodeId -> [ [label] ] }
                .mapValues { it -> it.value.flatten() } // { nodeId -> [label] }


        updatedNodes = updatedNodes.plus(assignedLabels
                .map { it.node() }
                .toSet() )

        updatedNodes = updatedNodes.plus(removedLabels
                .map { it.node() }
                .toSet() )

        nodeLabels = assignedPreviousLabels.plus(removedPreviousLabels)
        return this
    }

    fun withNodeProperties(assignedNodeProperties: Iterable<PropertyEntry<Node>>, removedNodeProperties: Iterable<PropertyEntry<Node>>): PreviousTransactionDataBuilder {
        val assignedPreviousProps = assignedNodeProperties
                .filter { it -> it.previouslyCommitedValue() != null }
                .map { it -> Pair(it.entity().id, Pair(it.key(), it.previouslyCommitedValue())) }
                .groupBy({it.first},{it.second}) // { nodeId -> [(k,v)] }
                .mapValues { it -> it.value.toMap() }

        val removedPreviousProps = removedNodeProperties
                .map { it -> Pair(it.entity().id, Pair(it.key(), it.previouslyCommitedValue())) }
                .groupBy({it.first},{it.second}) // { nodeId -> [(k,v)] }
                .mapValues { it -> it.value.toMap() }

        updatedNodes = updatedNodes.plus(assignedNodeProperties
                .map { it.entity() }
                .toSet() )

        updatedNodes = updatedNodes.plus(removedNodeProperties
                .map { it.entity() }
                .toSet() )

        nodeProperties = removedPreviousProps.plus(assignedPreviousProps)

        return this
    }

    fun withCreatedPayloads(createdPayload: List<NodePayload>): PreviousTransactionDataBuilder {
        this.createdPayload = createdPayload
        return this
    }

    fun withDeletedPayloads(deletedPayload: List<NodePayload>): PreviousTransactionDataBuilder {
        this.deletedPayload = deletedPayload
        return this
    }
}