package streams

import org.neo4j.graphdb.event.TransactionData
import org.neo4j.graphdb.event.TransactionEventHandler
import streams.events.*
import streams.extensions.labelNames
import streams.utils.SchemaUtils.getNodeKeys
import java.net.InetAddress
import java.util.concurrent.atomic.AtomicInteger


class StreamsTransactionEventHandler(private val router: StreamsEventRouter,
                                     private val streamsConstraintsService: StreamsConstraintsService,
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
            nodeConstraints: Map<String, Set<Constraint>>, relConstraints: Map<String, Set<Constraint>>) : List<StreamsTransactionEvent> {

        val getNodeConstraintsByLabels: (Collection<String>?) -> Set<Constraint> = { labels ->
            labels.orEmpty()
                    .flatMap { label -> nodeConstraints[label].orEmpty() }
                    .toSet()
        }

        return payloads.map { payload ->
            accumulator.incrementAndGet()
            val schema = if (payload is NodePayload) {
                val constraints = (payload.after ?: payload.before)!!.labels
                        .orEmpty()
                        .flatMap { label -> nodeConstraints[label].orEmpty() }
                        .toSet()
                SchemaBuilder()
                        .withPayload(payload)
                        .withConstraints(constraints)
                        .build()
            } else  {
                val relationshipPayload = (payload as RelationshipPayload)
                val relType = relationshipPayload.label
                val constraints = (relConstraints[relType].orEmpty()
                        + getNodeConstraintsByLabels(relationshipPayload.start.labels)
                        + getNodeConstraintsByLabels(relationshipPayload.end.labels))
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
                .map { it.entity().id to (it.key() to it.previouslyCommitedValue()) }
                .groupBy({it.first},{it.second}) // { nodeId -> [(k,v)] }
                .mapValues { it.value.toMap() }

        val deletedLabels = txd.removedLabels()
                .filter { txd.deletedNodes().contains( it.node() )}
                .map { labelEntry -> labelEntry.node().id to labelEntry.label().name() } // [ (nodeId, [label]) ]
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

    private fun buildRelationshipChanges(txd: TransactionData, builder: PreviousTransactionDataBuilder, nodeConstraints: Map<String, Set<Constraint>>): PreviousTransactionDataBuilder{
        val deletedRelProperties = txd.removedRelationshipProperties()
                .filter { txd.deletedRelationships().contains( it.entity() )}
                .map { it.entity().id to (it.key() to it.previouslyCommitedValue()) }
                .groupBy({ it.first }, { it.second }) // { nodeId -> [(k,v)] }
                .mapValues { it.value.toMap() }

        val nodeConstraintsCache = mutableMapOf<List<String>, List<Constraint>>()
        val filterNodeConstraintCache : (List<String>) -> List<Constraint> = { startLabels ->
            nodeConstraintsCache.computeIfAbsent(startLabels) {
                nodeConstraints
                        .filterKeys { startLabels.contains(it) }
                        .values
                        .flatten()
            }
        }

        val createdRelPayload = txd.createdRelationships().map {

            val afterRel = RelationshipChangeBuilder()
                    .withProperties(it.allProperties)
                    .build()

            val startLabels = it.startNode.labelNames()
            val startNodeConstraints = filterNodeConstraintCache(startLabels)
            val startKeys = getNodeKeys(startLabels, it.startNode.propertyKeys.toSet(), startNodeConstraints)
                    .toTypedArray()

            val endLabels = it.endNode.labelNames()
            val endNodeConstraints = filterNodeConstraintCache(endLabels)
            val endKeys = getNodeKeys(endLabels, it.endNode.propertyKeys.toSet(), endNodeConstraints)
                    .toTypedArray()

            val payload = RelationshipPayloadBuilder()
                    .withId(it.id.toString())
                    .withName(it.type.name())
                    .withStartNode(it.startNode.id.toString(), startLabels, it.startNode.getProperties(*startKeys))
                    .withEndNode(it.endNode.id.toString(), endLabels, it.endNode.getProperties(*endKeys))
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
            val isEndNodeDeleted = txd.isDeleted(it.endNode)

            val startNodeLabels = if (isStartNodeDeleted) builder.deletedLabels(it.startNode.id) else it.startNode.labelNames()
            val endNodeLabels = if (isEndNodeDeleted) builder.deletedLabels(it.endNode.id) else it.endNode.labelNames()

            val startPropertyKeys = if (isStartNodeDeleted) {
                builder.nodeDeletedPayload(it.startNodeId)?.before?.properties?.keys.orEmpty()
            } else {
                it.startNode.propertyKeys
            }

            val endPropertyKeys = if (isEndNodeDeleted) {
                builder.nodeDeletedPayload(it.endNodeId)?.before?.properties?.keys.orEmpty()
            } else {
                it.endNode.propertyKeys
            }

            val startNodeConstraints = filterNodeConstraintCache(startNodeLabels)
            val startKeys = getNodeKeys(startNodeLabels, startPropertyKeys.toSet(), startNodeConstraints)

            val endNodeConstraints = filterNodeConstraintCache(endNodeLabels)
            val endKeys = getNodeKeys(endNodeLabels, endPropertyKeys.toSet(), endNodeConstraints)

            val startProperties = if (isStartNodeDeleted) {
                val payload = builder.nodeDeletedPayload(it.startNode.id)!!
                (payload.after ?: payload.before)?.properties?.filterKeys { startKeys.contains(it) }.orEmpty()
            } else {
                it.startNode.getProperties(*startKeys.toTypedArray())
            }
            val endProperties = if (isEndNodeDeleted) {
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
        val nodeConstraints = streamsConstraintsService.allForLabels()
        val relConstraints = streamsConstraintsService.allForRelationshipType()
        var builder = PreviousTransactionDataBuilder()
                .withNodeConstraints(nodeConstraints)
                .withRelConstraints(relConstraints)

        builder = buildNodeChanges(txd, builder)
        builder = buildRelationshipChanges(txd, builder, nodeConstraints)

        return builder.build()
    }
}