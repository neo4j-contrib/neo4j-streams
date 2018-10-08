package streams.events

import org.neo4j.graphdb.Node
import org.neo4j.graphdb.event.LabelEntry
import org.neo4j.graphdb.event.PropertyEntry

data class PreviousTransactionData(val nodeProperties : Map<Long,Map<String,Any>>,
                                   val nodeLabels: Map<Long, List<String>>,
                                   val updatedNodeIds : Set<Long>)

/**
 * Build a data class containing the previous (before) state of the nodes/relationships
 */
class PreviousTransactionDataBuilder(){

    private var updatedNodeIds : Set<Long> = emptySet()
    private var nodeProperties : Map<Long,Map<String,Any>> = emptyMap()
    private var nodeLabels: Map<Long, List<String>> = emptyMap()

    fun build() : PreviousTransactionData{
        val updatedNodeIds = updatedNodeIds.plus(nodeProperties.keys).plus(nodeLabels.keys)
        return PreviousTransactionData(nodeProperties, nodeLabels , updatedNodeIds)
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

        // mark the nodeIds where new properties added
        updatedNodeIds = assignedNodeProperties
                .filter { it -> it.previouslyCommitedValue() == null }
                .map { it.entity().id }
                .toSet()

        nodeProperties = removedPreviousProps.plus(assignedPreviousProps)
        return this
    }
}