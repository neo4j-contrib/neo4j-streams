package streams.events

import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.event.LabelEntry
import org.neo4j.graphdb.event.PropertyEntry
import streams.extensions.labelNames

data class PreviousNodeTransactionData(val nodeProperties: Map<Long, Map<String, Any>>,
                                   val nodeLabels: Map<Long, List<String>>,
                                   val updatedPayloads: List<NodePayload> = emptyList(),
                                   val createdPayload: List<NodePayload>,
                                   val deletedPayload: List<NodePayload>)

data class PreviousRelTransactionData(val relProperties: Map<Long, Map<String, Any>> = emptyMap(),
                                       val relTypes: Map<Long,String> = emptyMap(),
                                       val updatedPayloads: List<RelationshipPayload> = emptyList(),
                                       val createdPayload: List<RelationshipPayload> = emptyList(),
                                       val deletedPayload: List<RelationshipPayload> = emptyList())

data class PreviousTransactionData(val nodeData: PreviousNodeTransactionData,
                                   val relData: PreviousRelTransactionData)

/**
 * Build a data class containing the previous (before) state of the nodes/relationships
 */
class PreviousTransactionDataBuilder(){

    //nodes
    private var nodeProperties : Map<Long,Map<String,Any>> = emptyMap()
    private var nodeLabels: Map<Long, List<String>> = emptyMap()
    private var updatedNodes : Set<Node> = emptySet()
    private var nodeCreatedPayload: List<NodePayload> = emptyList()
    private var nodeDeletedPayload: List<NodePayload> = emptyList()
    private var deletedLabels: Map<Long, List<String>> = emptyMap()

    //relationships
    private var relProperties : Map<Long,Map<String,Any>> = emptyMap()
    private var relTypes: Map<Long, String> = emptyMap()
    private var updatedRels : Set<Relationship> = emptySet()
    private var relCreatedPayload: List<RelationshipPayload> = emptyList()
    private var relDeletedPayload: List<RelationshipPayload> = emptyList()

    fun build() : PreviousTransactionData{

        var createdNodeIds = hashSetOf<String>()
        nodeCreatedPayload.forEach({
            createdNodeIds.add(it.id)
        })

        //val createdIds = nodeCreatedPayload.map { it.id }.toSet()

        val updatedPayloads = updatedNodes
                .filter { ! createdNodeIds.contains(it.id.toString()) }
                .map { it ->
                    //val labelsBefore = nodeLabels.getOrDefault(it.id, emptyList())
                    val labelsBefore = nodeLabels.getOrDefault(it.id, it.labelNames())
                    val propsBefore = nodeProperties.getOrDefault(it.id, emptyMap())

                    val beforeNode = NodeChangeBuilder()
                            .withLabels(labelsBefore)
                            .withProperties(propsBefore)
                            .build()

                    val labelsAfter = it.labelNames()

                    val afterNode = NodeChangeBuilder()
                            .withLabels(labelsAfter)
                            .withProperties(it.allProperties)
                            .build()

                    val payload = NodePayloadBuilder()
                            .withId(it.id.toString())
                            .withBefore(beforeNode)
                            .withAfter(afterNode)
                            .build()

                    payload
                }

        val nodeData = PreviousNodeTransactionData(nodeProperties, nodeLabels , updatedPayloads,nodeCreatedPayload, nodeDeletedPayload)

        var notUpdatedRels = hashSetOf<String>()
        relCreatedPayload.forEach({
            notUpdatedRels.add(it.id)
        })

        relDeletedPayload.forEach({
            notUpdatedRels.add(it.id)
        })

        val updatedRelPayloads = updatedRels
                .filter { ! notUpdatedRels.contains(it.id.toString()) }
                .map { it ->
                    val propsBefore = relProperties.getOrDefault(it.id, emptyMap())

                    val beforeNode = RelationshipChangeBuilder()
                            .withProperties(propsBefore)
                            .build()

                    val afterNode = RelationshipChangeBuilder()
                            .withProperties(it.allProperties)
                            .build()

                    val payload = RelationshipPayloadBuilder()
                            .withId(it.id.toString())
                            .withName(it.type.name())
                            .withStartNode(it.startNode.id.toString(), it.startNode.labelNames())
                            .withEndNode(it.endNode.id.toString(), it.endNode.labelNames())
                            .withBefore(beforeNode)
                            .withAfter(afterNode)
                            .build()

                    payload
                }

        val relData = PreviousRelTransactionData(createdPayload = this.relCreatedPayload, deletedPayload = this.relDeletedPayload, updatedPayloads = updatedRelPayloads)

        return PreviousTransactionData(nodeData=nodeData, relData = relData)
    }

    fun withLabels(assignedLabels: Iterable<LabelEntry>, removedLabels: Iterable<LabelEntry>): PreviousTransactionDataBuilder {
        val assignedPreviousLabels = assignedLabels
                .map { labelEntry -> Pair(labelEntry.node().id, labelEntry.node().labels.filter { it != labelEntry.label() }.map { it -> it.name() }.toList()) } // [ (nodeId, [label]) ]
                .groupBy({it.first},{it.second}) // { nodeId -> [ [label] ] }
                .mapValues { it -> it.value.flatten() } // { nodeId -> [label] }

        val removedPreviousLabels = removedLabels
                .map { labelEntry -> Pair(labelEntry.node().id, labelEntry.node().labelNames().toList().plus(labelEntry.label().name())) } // [ (nodeId, [label]) ]
                .groupBy({it.first},{it.second}) // { nodeId -> [ [label] ] }
                .mapValues { it -> it.value.flatten() } // { nodeId -> [label] }


        updatedNodes = updatedNodes.plus(assignedLabels
                .map { it.node() }
                .toSet() )

        updatedNodes = updatedNodes.plus(removedLabels
                .map { it.node() }
                .toSet() )

        nodeLabels = assignedPreviousLabels.plus(removedPreviousLabels)

        val allProps = mutableMapOf<Long, MutableMap<String, Any>>()
        updatedNodes.forEach({
            allProps.putIfAbsent(it.id, it.allProperties)
        })

        nodeProperties = nodeProperties.plus(allProps)

        return this
    }

    fun withNodeProperties(assignedNodeProperties: Iterable<PropertyEntry<Node>>, removedNodeProperties: Iterable<PropertyEntry<Node>>): PreviousTransactionDataBuilder {
        val allProps = mutableMapOf<Long, MutableMap<String, Any>>()
        assignedNodeProperties.filter { it -> it.previouslyCommitedValue() == null }
                .forEach({
                    var props = allProps.getOrDefault(it.entity().id, it.entity().allProperties.toMutableMap())
                    props.remove(it.key())
                    allProps.putIfAbsent(it.entity().id, props)
                })

        assignedNodeProperties.filter { it -> it.previouslyCommitedValue() != null }
                .forEach({
                    var props = allProps.getOrDefault(it.entity().id, it.entity().allProperties.toMutableMap())
                    props.put(it.key(), it.previouslyCommitedValue())
                    allProps.putIfAbsent(it.entity().id, props)
                })

        removedNodeProperties.forEach({
            var props = allProps.getOrDefault(it.entity().id, it.entity().allProperties.toMutableMap())
            props.put(it.key(), it.previouslyCommitedValue())
            allProps.putIfAbsent(it.entity().id, props)
        })

        updatedNodes = updatedNodes.plus(assignedNodeProperties
                .map { it.entity() }
                .toSet() )

        updatedNodes = updatedNodes.plus(removedNodeProperties
                .map { it.entity() }
                .toSet() )

        //val updateNodesLabels = updatedNodes.groupBy ( {it.id},{it.labels.map { it.name() }}).mapValues { it -> it.value.flatten() }

        //nodeLabels = nodeLabels.plus(updateNodesLabels)

        nodeProperties = nodeProperties.plus(allProps)

        return this
    }

    fun withNodeCreatedPayloads(createdPayload: List<NodePayload>): PreviousTransactionDataBuilder {
        this.nodeCreatedPayload = createdPayload
        return this
    }

    fun withNodeDeletedPayloads(deletedPayload: List<NodePayload>): PreviousTransactionDataBuilder {
        this.nodeDeletedPayload = deletedPayload
        return this
    }

    fun withRelCreatedPayloads(createdPayload: List<RelationshipPayload>): PreviousTransactionDataBuilder {
        this.relCreatedPayload = createdPayload
        return this
    }

    fun withRelDeletedPayloads(deletedPayload: List<RelationshipPayload>): PreviousTransactionDataBuilder {
        this.relDeletedPayload = deletedPayload
        return this
    }

    fun withRelProperties(assignedRelProperties: Iterable<PropertyEntry<Relationship>>, removedRelProperties: Iterable<PropertyEntry<Relationship>>): PreviousTransactionDataBuilder {
        val allProps = mutableMapOf<Long, MutableMap<String, Any>>()
        assignedRelProperties.filter { it -> it.previouslyCommitedValue() == null }
                .forEach({
                    var props = allProps.getOrDefault(it.entity().id, it.entity().allProperties.toMutableMap())
                    props.remove(it.key())
                    allProps.putIfAbsent(it.entity().id, props)
                })

        assignedRelProperties.filter { it -> it.previouslyCommitedValue() != null }
                .forEach({
                    var props = allProps.getOrDefault(it.entity().id, it.entity().allProperties.toMutableMap())
                    props.put(it.key(), it.previouslyCommitedValue())
                    allProps.putIfAbsent(it.entity().id, props)
                })

        removedRelProperties.forEach({
            var props = allProps.getOrDefault(it.entity().id, it.entity().allProperties.toMutableMap())
            props.put(it.key(), it.previouslyCommitedValue())
            allProps.putIfAbsent(it.entity().id, props)
        })

        updatedRels = updatedRels.plus(assignedRelProperties
                .map { it.entity() }
                .toSet() )

        updatedRels = updatedRels.plus(removedRelProperties
                .map { it.entity() }
                .toSet() )

        relProperties = relProperties.plus(allProps)

        return this
    }

    fun withDeletedLabels(deletedLabels: Map<Long, List<String>>): PreviousTransactionDataBuilder {
        this.deletedLabels = deletedLabels
        return this
    }

    fun deletedLabels(id : Long): List<String>{
        return this.deletedLabels.getOrDefault(id, emptyList())
    }
}