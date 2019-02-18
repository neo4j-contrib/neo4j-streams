package streams

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.event.TransactionData
import org.neo4j.graphdb.event.TransactionEventHandler
import org.neo4j.graphdb.schema.ConstraintType
import streams.events.*
import streams.extensions.labelNames
import java.net.InetAddress
import java.util.concurrent.atomic.AtomicInteger


class StreamsTransactionEventHandler(private val db: GraphDatabaseService,
                                     private val router: StreamsEventRouter,
                                     private val configuration: StreamsEventRouterConfiguration)
    : TransactionEventHandler<PreviousTransactionData> {

    /**
     * Wrap the payload into a StreamsTransactionEvent for the eventId
     */
    private fun payloadToEvent(operation: OperationType, payload: Payload, schema: Schema, txd: TransactionData, eventId: Int, eventCount: Int) : StreamsTransactionEvent{
        val meta = StreamsEventMetaBuilder()
                .withOperation(operation)
                .withTransactionEventId(eventId)
                .withTransactionEventsCount(eventCount)
                .withUsername(txd.username())
                .withTimestamp(txd.commitTime)
                .withTransactionId(txd.transactionId)
                .withHostname(InetAddress.getLocalHost().hostName)
                .build()

        val builder = StreamsTransactionEventBuilder()
                .withMeta(meta)
                .withPayload(payload)
                .withSchema(schema)

        return builder.build()
    }

    private fun mapToStreamsEvent(operation: OperationType, payloads: List<Payload>, txd: TransactionData, totalEventsCount: Int, accumulator: AtomicInteger,
            nodeConstraints: Map<Label, Set<Constraint>>, relConstraints: Map<RelationshipType, Set<Constraint>>) : List<StreamsTransactionEvent> {
        return payloads.map { payload ->
            accumulator.incrementAndGet()
            val schema = if (payload is NodePayload) {
                val labels = (payload.after ?: payload.before)!!.labels?.toSet().orEmpty()
                val constraints = if (labels.isNotEmpty()) {
                    labels.flatMap { label ->
                        nodeConstraints[Label.label(label)].orEmpty()
                    }.toSet()
                } else {
                    emptySet()
                }
                SchemaBuilder()
                        .withPayload(payload)
                        .withConstraints(constraints)
                        .build()
            } else  {
                val relationshipPayload = (payload as RelationshipPayload)
                val label = relationshipPayload.label
                val constraints = db.beginTx().use {
                    val start = payload.start.labels
                            ?.flatMap { label ->
                                nodeConstraints[Label.label(label)].orEmpty()
                            }
                            .orEmpty()
                    val end = payload.end.labels
                            ?.flatMap { label ->
                                nodeConstraints[Label.label(label)].orEmpty()
                            }
                            .orEmpty()
                    val rel = relConstraints[RelationshipType.withName(label)].orEmpty()
                    start + end + rel
                }.toSet()
                SchemaBuilder()
                        .withPayload(payload)
                        .withConstraints(constraints)
                        .build()
            }
            payloadToEvent(operation, payload, schema, txd, accumulator.get(), totalEventsCount)
        }
    }

    private fun buildNodeChanges(txd: TransactionData, builder:PreviousTransactionDataBuilder):PreviousTransactionDataBuilder{

        val createdPayload = txd.createdNodes().map {
            val labels = it.labelNames()

            val afterNode = NodeChangeBuilder()
                    .withLabels(labels)
                    .withProperties(it.allProperties)
                    .build()

            val payload = NodePayloadBuilder()
                    .withId(it.id.toString())
                    .withAfter(afterNode)
                    .build()

            payload
        }

        // labels and properties of deleted nodes are unreachable
        val deletedNodeProperties = txd.removedNodeProperties()
                .filter { txd.deletedNodes().contains( it.entity() )}
                .map { Pair(it.entity().id, Pair(it.key(), it.previouslyCommitedValue())) }
                .groupBy({it.first},{it.second}) // { nodeId -> [(k,v)] }
                .mapValues { it.value.toMap() }

        val deletedLabels = txd.removedLabels()
                .filter { txd.deletedNodes().contains( it.node() )}
                .map { labelEntry -> Pair(labelEntry.node().id, labelEntry.label().name()) } // [ (nodeId, [label]) ]
                .groupBy({it.first},{it.second}) // { nodeId -> [label]  }


        val removedNodeProperties = txd.removedNodeProperties().
                filter { !txd.deletedNodes().contains( it.entity() )}
        val removedLabels = txd.removedLabels()
                .filter { !txd.deletedNodes().contains( it.node() )}

        val deletedPayload = txd.deletedNodes().map {

            val beforeNode = NodeChangeBuilder()
                    .withLabels(deletedLabels.getOrDefault(it.id, emptyList()))
                    .withProperties(deletedNodeProperties.getOrDefault(it.id, emptyMap()))
                    .build()

            val payload = NodePayloadBuilder()
                    .withId(it.id.toString())
                    .withBefore(beforeNode)
                    .build()

            payload
        }

        //don't change the order of the with methods
        return builder.withLabels(txd.assignedLabels(),removedLabels)
                .withNodeProperties(txd.assignedNodeProperties(),removedNodeProperties)
                .withNodeCreatedPayloads(createdPayload)
                .withNodeDeletedPayloads(deletedPayload)
                .withDeletedLabels(deletedLabels)
    }

    private fun buildRelationshipChanges(txd: TransactionData, builder: PreviousTransactionDataBuilder, nodeConstraints: MutableMap<Label, Set<Constraint>>): PreviousTransactionDataBuilder{
        val deletedRelProperties = txd.removedRelationshipProperties()
                .filter { txd.deletedRelationships().contains( it.entity() )}
                .map { Pair(it.entity().id, Pair(it.key(), it.previouslyCommitedValue())) }
                .groupBy({ it.first }, { it.second }) // { nodeId -> [(k,v)] }
                .mapValues { it.value.toMap() }

        val createdRelPayload = txd.createdRelationships().map {

            val afterRel = RelationshipChangeBuilder()
                    .withProperties(it.allProperties)
                    .build()

            val startKeys = it.startNode.labels.flatMap { label -> nodeConstraints[label].orEmpty() }.flatMap { it.properties }.toSet().toTypedArray()
            val endKeys = it.endNode.labels.flatMap { label -> nodeConstraints[label].orEmpty() }.flatMap { it.properties }.toSet().toTypedArray()

            val payload = RelationshipPayloadBuilder()
                    .withId(it.id.toString())
                    .withName(it.type.name())
                    .withStartNode(it.startNode.id.toString(), it.startNode.labelNames(), it.startNode.getProperties(*startKeys))
                    .withEndNode(it.endNode.id.toString(), it.endNode.labelNames(), it.endNode.getProperties(*endKeys))
                    .withAfter(afterRel)
                    .build()

            payload
        }

        val deletedRelPayload = txd.deletedRelationships().map {
            val beforeRel = RelationshipChangeBuilder()
                    .withProperties(deletedRelProperties.getOrDefault(it.id, emptyMap()))
                    .build()

            // start and end can be unreachable in case of detach delete
            val isStartNodeDeleted = txd.isDeleted(it.startNode)
            val isEndNodeDelete = txd.isDeleted(it.endNode)

            val startNodeLabels = if (isStartNodeDeleted) builder.deletedLabels(it.startNode.id) else it.startNode.labelNames()
            val endNodeLabels = if (isEndNodeDelete) builder.deletedLabels(it.endNode.id) else it.endNode.labelNames()

            val startKeys = startNodeLabels.flatMap { label -> nodeConstraints[Label.label(label)].orEmpty() }.flatMap { it.properties }.toSet() //.toTypedArray()
            val endKeys = endNodeLabels.flatMap { label -> nodeConstraints[Label.label(label)].orEmpty() }.flatMap { it.properties }.toSet() //.toTypedArray()

            val startProperties = if (isStartNodeDeleted) {
                val payload = builder.nodeDeletedPayload(it.startNode.id)!!
                (payload.after ?: payload.before)?.properties?.filterKeys { startKeys.contains(it) }.orEmpty()
            } else {
                it.startNode.getProperties(*startKeys.toTypedArray())
            }
            val endProperties = if (isEndNodeDelete) {
                val payload = builder.nodeDeletedPayload(it.endNode.id)!!
                (payload.after ?: payload.before)?.properties?.filterKeys { endKeys.contains(it) }.orEmpty()
            } else {
                it.endNode.getProperties(*endKeys.toTypedArray())
            }

            val payload = RelationshipPayloadBuilder()
                    .withId(it.id.toString())
                    .withName(it.type.name())
                    .withStartNode(it.startNode.id.toString(), startNodeLabels, startProperties)
                    .withEndNode(it.endNode.id.toString(), endNodeLabels, endProperties)
                    .withBefore(beforeRel)
                    .build()

            payload
        }

        val removedRelsProperties = txd.removedRelationshipProperties().filter { !txd.deletedRelationships().contains( it.entity() )}

        //don't change the order of the with methods
        return builder.withRelProperties(txd.assignedRelationshipProperties(), removedRelsProperties)
                .withRelCreatedPayloads(createdRelPayload)
                .withRelDeletedPayloads(deletedRelPayload)
    }

    override fun afterRollback(p0: TransactionData?, p1: PreviousTransactionData?) {}

    override fun afterCommit(txd: TransactionData, previousTxd: PreviousTransactionData) {

        val nodePrevious = previousTxd.nodeData
        val relPrevious = previousTxd.relData

        val totalEventsCount = nodePrevious.createdPayload.size + nodePrevious.deletedPayload.size + nodePrevious.updatedPayloads.size +
                relPrevious.createdPayload.size + relPrevious.deletedPayload.size + relPrevious.updatedPayloads.size

        val eventAcc = AtomicInteger(-1)
        val events = mutableListOf<StreamsTransactionEvent>()
        events.addAll(mapToStreamsEvent(OperationType.created, nodePrevious.createdPayload, txd, totalEventsCount, eventAcc,
                previousTxd.nodeConstraints, previousTxd.relConstraints))
        events.addAll(mapToStreamsEvent(OperationType.deleted, nodePrevious.deletedPayload, txd, totalEventsCount, eventAcc,
                previousTxd.nodeConstraints, previousTxd.relConstraints))
        events.addAll(mapToStreamsEvent(OperationType.updated, nodePrevious.updatedPayloads, txd, totalEventsCount, eventAcc,
                previousTxd.nodeConstraints, previousTxd.relConstraints))
        events.addAll(mapToStreamsEvent(OperationType.created, relPrevious.createdPayload, txd, totalEventsCount, eventAcc,
                previousTxd.nodeConstraints, previousTxd.relConstraints))
        events.addAll(mapToStreamsEvent(OperationType.deleted, relPrevious.deletedPayload, txd, totalEventsCount, eventAcc,
                previousTxd.nodeConstraints, previousTxd.relConstraints))
        events.addAll(mapToStreamsEvent(OperationType.updated, relPrevious.updatedPayloads, txd, totalEventsCount, eventAcc,
                previousTxd.nodeConstraints, previousTxd.relConstraints))

        val topicEventsMap = events.flatMap { event ->
                    val map  = when (event.payload.type) {
                        EntityType.node -> NodeRoutingConfiguration.prepareEvent(event, configuration.nodeRouting)
                        EntityType.relationship -> RelationshipRoutingConfiguration.prepareEvent(event, configuration.relRouting)
                    }
            map.map { it.key to it.value }
                }
                .groupBy({ it.first }, { it.second })

        topicEventsMap.forEach {
            router.sendEvents(it.key, it.value)
        }
    }

    override fun beforeCommit(txd: TransactionData): PreviousTransactionData {

        val nodeConstraints = mutableMapOf<Label, Set<Constraint>>()
        val relConstraints = mutableMapOf<RelationshipType, Set<Constraint>>()
        db.beginTx().use {
            db.schema().constraints
                    .filter { try { it.label; true } catch (e: IllegalStateException) { false } }
                    .groupBy { it.label }
                    .forEach { label, constraints ->
                        nodeConstraints[label] = constraints.map { Constraint(label.name(), it.propertyKeys.toSet(), it.constraintType) }.toSet()
                    }
            db.schema().constraints
                    .filter { try { it.relationshipType; true } catch (e: IllegalStateException) { false } }
                    .groupBy { it.relationshipType }
                    .forEach { relationshipType, constraints ->
                        relConstraints[relationshipType] = constraints.map { Constraint(relationshipType.name(), it.propertyKeys.toSet(), it.constraintType) }.toSet()
                    }
        }

        var builder = PreviousTransactionDataBuilder()
                .withNodeConstraints(nodeConstraints)
                .withRelConstraints(relConstraints)

        builder = buildNodeChanges(txd, builder)
        builder = buildRelationshipChanges(txd, builder, nodeConstraints)

        return builder.build()
    }
}