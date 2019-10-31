package streams.events

import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.event.LabelEntry
import org.neo4j.graphdb.event.PropertyEntry
import streams.extensions.labelNames
import streams.utils.SchemaUtils.getNodeKeys

data class PreviousNodeTransactionData(val nodeProperties: Map<Long, Map<String, Any>>,
                                   val nodeLabels: Map<Long, List<String>>,
                                   val updatedPayloads: List<NodePayload> = emptyList(),
                                   val createdPayload: List<NodePayload>,
                                   val deletedPayload: List<NodePayload>)

data class PreviousRelTransactionData(val relProperties: Map<Long, Map<String, Any>> = emptyMap(),
                                       val updatedPayloads: List<RelationshipPayload> = emptyList(),
                                       val createdPayload: List<RelationshipPayload> = emptyList(),
                                       val deletedPayload: List<RelationshipPayload> = emptyList())

data class PreviousTransactionData(val nodeData: PreviousNodeTransactionData,
                                   val relData: PreviousRelTransactionData,
                                   val nodeConstraints: Map<String, Set<Constraint>>,
                                   val relConstraints: Map<String, Set<Constraint>>)


/**
 * Build a data class containing the previous (before) state of the nodes/relationships
 */
class PreviousTransactionDataBuilder {

    //nodes
    private var nodeProperties : Map<Long,Map<String,Any>> = emptyMap()
    private var nodeLabels: Map<Long, List<String>> = emptyMap()
    private var updatedNodes : Set<Node> = emptySet()
    private var nodeCreatedPayload: List<NodePayload> = emptyList()
    private var nodeDeletedPayload: List<NodePayload> = emptyList()
    private var deletedLabels: Map<Long, List<String>> = emptyMap()

    //relationships
    private var relProperties : Map<Long,Map<String,Any>> = emptyMap()
    private var updatedRels : Set<Relationship> = emptySet()
    private var relCreatedPayload: List<RelationshipPayload> = emptyList()
    private var relDeletedPayload: List<RelationshipPayload> = emptyList()

    private lateinit var nodeConstraints: Map<String, Set<Constraint>>
    private lateinit var relConstraints: Map<String, Set<Constraint>>

    fun withNodeConstraints(nodeConstraints: Map<String, Set<Constraint>>): PreviousTransactionDataBuilder {
        this.nodeConstraints = nodeConstraints
        return this
    }

    fun withRelConstraints(relConstraints: Map<String, Set<Constraint>>): PreviousTransactionDataBuilder {
        this.relConstraints = relConstraints
        return this
    }

    fun build() : PreviousTransactionData{
        var createdNodeIds = hashSetOf<String>()
        nodeCreatedPayload.forEach {
            createdNodeIds.add(it.id)
        }

        val updatedPayloads = updatedNodes
                .filter { ! createdNodeIds.contains(it.id.toString()) }
                .map {
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
        relCreatedPayload.forEach {
            notUpdatedRels.add(it.id)
        }

        relDeletedPayload.forEach {
            notUpdatedRels.add(it.id)
        }

        val nodeConstraintsCache = mutableMapOf<List<String>, List<Constraint>>()

        val updatedRelPayloads = updatedRels
                .filter { ! notUpdatedRels.contains(it.id.toString()) }
                .map {
                    val propsBefore = relProperties.getOrDefault(it.id, emptyMap())

                    val beforeNode = RelationshipChangeBuilder()
                            .withProperties(propsBefore)
                            .build()

                    val afterNode = RelationshipChangeBuilder()
                            .withProperties(it.allProperties)
                            .build()

                    val startLabels = it.startNode.labelNames()
                    val startNodeConstraints = nodeConstraintsCache.computeIfAbsent(startLabels) {
                        nodeConstraints
                                .filterKeys { startLabels.contains(it) }
                                .flatMap { it.value }
                    }
                    val startNodeKeys = getNodeKeys(startLabels, it.startNode.propertyKeys.toSet(), startNodeConstraints)
                            .toTypedArray()


                    val endLabels = it.endNode.labelNames()
                    val endNodeConstraints = nodeConstraintsCache.computeIfAbsent(endLabels) {
                        nodeConstraints
                                .filterKeys { endLabels.contains(it) }
                                .flatMap { it.value }
                    }
                    val endNodeKeys = getNodeKeys(endLabels, it.endNode.propertyKeys.toSet(), endNodeConstraints)
                            .toTypedArray()

                    val payload = RelationshipPayloadBuilder()
                            .withId(it.id.toString())
                            .withName(it.type.name())
                            .withStartNode(it.startNode.id.toString(), startLabels, it.startNode.getProperties(*startNodeKeys))
                            .withEndNode(it.endNode.id.toString(), endLabels, it.endNode.getProperties(*endNodeKeys))
                            .withBefore(beforeNode)
                            .withAfter(afterNode)
                            .build()

                    payload
                }

        val relData = PreviousRelTransactionData(createdPayload = this.relCreatedPayload, deletedPayload = this.relDeletedPayload, updatedPayloads = updatedRelPayloads)

        return PreviousTransactionData(nodeData = nodeData, relData = relData, nodeConstraints = nodeConstraints, relConstraints = relConstraints)
    }

    fun withLabels(assignedLabels: Iterable<LabelEntry>, removedLabels: Iterable<LabelEntry>): PreviousTransactionDataBuilder {
        val assignedPreviousLabels = assignedLabels
                .map { labelEntry -> Pair(labelEntry.node().id, labelEntry.node().labels.filter { it != labelEntry.label() }.map { it.name() }.toList()) } // [ (nodeId, [label]) ]
                .groupBy({it.first},{it.second}) // { nodeId -> [ [label] ] }
                .mapValues { it.value.flatten() } // { nodeId -> [label] }

        val removedPreviousLabels = removedLabels
                .map { labelEntry -> Pair(labelEntry.node().id, labelEntry.node().labelNames().toList().plus(labelEntry.label().name())) } // [ (nodeId, [label]) ]
                .groupBy({it.first},{it.second}) // { nodeId -> [ [label] ] }
                .mapValues { it.value.flatten() } // { nodeId -> [label] }


        updatedNodes = updatedNodes.plus(assignedLabels
                .map { it.node() }
                .toSet() )

        updatedNodes = updatedNodes.plus(removedLabels
                .map { it.node() }
                .toSet() )

        nodeLabels = assignedPreviousLabels.plus(removedPreviousLabels)

        val allProps = mutableMapOf<Long, MutableMap<String, Any>>()
        updatedNodes.forEach {
            allProps.putIfAbsent(it.id, it.allProperties)
        }

        nodeProperties = nodeProperties.plus(allProps)

        return this
    }

    fun withNodeProperties(assignedNodeProperties: Iterable<PropertyEntry<Node>>, removedNodeProperties: Iterable<PropertyEntry<Node>>): PreviousTransactionDataBuilder {
        val allProps = mutableMapOf<Long, MutableMap<String, Any>>()
        assignedNodeProperties.filter { it.previouslyCommitedValue() == null }
                .forEach {
                    var props = allProps.getOrDefault(it.entity().id, it.entity().allProperties.toMutableMap())
                    props.remove(it.key())
                    allProps.putIfAbsent(it.entity().id, props)
                }

        assignedNodeProperties.filter { it.previouslyCommitedValue() != null }
                .forEach {
                    var props = allProps.getOrDefault(it.entity().id, it.entity().allProperties.toMutableMap())
                    props.put(it.key(), it.previouslyCommitedValue())
                    allProps.putIfAbsent(it.entity().id, props)
                }

        removedNodeProperties.forEach {
            var props = allProps.getOrDefault(it.entity().id, it.entity().allProperties.toMutableMap())
            props.put(it.key(), it.previouslyCommitedValue())
            allProps.putIfAbsent(it.entity().id, props)
        }

        updatedNodes = updatedNodes.plus(assignedNodeProperties
                .map { it.entity() }
                .toSet() )

        updatedNodes = updatedNodes.plus(removedNodeProperties
                .map { it.entity() }
                .toSet() )

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
        assignedRelProperties.filter { it.previouslyCommitedValue() == null }
                .forEach {
                    var props = allProps.getOrDefault(it.entity().id, it.entity().allProperties.toMutableMap())
                    props.remove(it.key())
                    allProps.putIfAbsent(it.entity().id, props)
                }

        assignedRelProperties.filter { it.previouslyCommitedValue() != null }
                .forEach {
                    var props = allProps.getOrDefault(it.entity().id, it.entity().allProperties.toMutableMap())
                    props.put(it.key(), it.previouslyCommitedValue())
                    allProps.putIfAbsent(it.entity().id, props)
                }

        removedRelProperties.forEach {
            var props = allProps.getOrDefault(it.entity().id, it.entity().allProperties.toMutableMap())
            props.put(it.key(), it.previouslyCommitedValue())
            allProps.putIfAbsent(it.entity().id, props)
        }

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

    fun nodeDeletedPayload(id: Long): NodePayload? {
        return this.nodeDeletedPayload.filter { it.id == id.toString() }.firstOrNull()
    }


}