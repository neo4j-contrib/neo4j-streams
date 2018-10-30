package streams

import org.neo4j.graphdb.event.TransactionData
import org.neo4j.graphdb.event.TransactionEventHandler
import streams.events.*
import java.net.InetAddress

class StreamsTransactionEventHandler(private val router : StreamsEventRouter) : TransactionEventHandler<PreviousTransactionData> {

    /**
     * Wrap the payload into a StreamsEvent for the eventId
     */
    fun payloadToEvent(operation: OperationType, payload: Payload, txd: TransactionData, eventId: Int, eventCount: Int) : StreamsEvent{
        val meta = StreamsEventMetaBuilder()
                .withOperation(operation)
                .withTransactionEventId(eventId)
                .withTransactionEventsCount(eventCount)
                .withUsername(txd.username())
                .withTimestamp(txd.commitTime)
                .withTransactionId(txd.transactionId)
                .withHostname(InetAddress.getLocalHost().hostName)
                .build()
        val schema = SchemaBuilder().build()

        val builder = StreamsEventBuilder()
                .withMeta(meta)
                .withPayload(payload)
                .withSchema(schema)

        val event = builder.build()

        return event
    }

    /**
     * Send a StreamsEvent by the router for each payload
     */
    fun sendPayloads(operation: OperationType, payloads: List<Payload>,txd: TransactionData, totalEventsCount: Int, offset: Int) : Int {
        var eventId = offset
        payloads.map { it ->
            payloadToEvent(operation, it, txd, eventId++, totalEventsCount)
        }.forEach(router::sendEvent)
        return eventId
    }

    fun buildNodeChanges(txd: TransactionData, builder:PreviousTransactionDataBuilder):PreviousTransactionDataBuilder{

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
        val deletedNodeProperties = txd.removedNodeProperties().filter { txd.deletedNodes().contains( it.entity() )}
                .map { it -> Pair(it.entity().id, Pair(it.key(), it.previouslyCommitedValue())) }
                .groupBy({it.first},{it.second}) // { nodeId -> [(k,v)] }
                .mapValues { it -> it.value.toMap() }

        val deletedLabels = txd.removedLabels().filter { txd.deletedNodes().contains( it.node() )}
                .map { labelEntry -> Pair(labelEntry.node().id, labelEntry.label().name()) } // [ (nodeId, [label]) ]
                .groupBy({it.first},{it.second}) // { nodeId -> [label]  }


        val removedNodeProperties = txd.removedNodeProperties().filter { !txd.deletedNodes().contains( it.entity() )}
        val removedLabels = txd.removedLabels().filter { !txd.deletedNodes().contains( it.node() )}

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

    fun buildRelationshipChanges(txd: TransactionData, builder:PreviousTransactionDataBuilder):PreviousTransactionDataBuilder{
        val deletedRelProperties = txd.removedRelationshipProperties().filter { txd.deletedRelationships().contains( it.entity() )}
                .map { it -> Pair(it.entity().id, Pair(it.key(), it.previouslyCommitedValue())) }
                .groupBy({it.first},{it.second}) // { nodeId -> [(k,v)] }
                .mapValues { it -> it.value.toMap() }

        val createdRelPayload = txd.createdRelationships().map {

            val afterRel = RelationshipChangeBuilder()
                    .withProperties(it.allProperties)
                    .build()

            val payload = RelationshipPayloadBuilder()
                    .withId(it.id.toString())
                    .withName(it.type.name())
                    .withStartNode(it.startNode.id.toString(), it.startNode.labelNames())
                    .withEndNode(it.endNode.id.toString(), it.endNode.labelNames())
                    .withAfter(afterRel)
                    .build()

            payload
        }

        val deletedRelPayload = txd.deletedRelationships().map {
            val beforeRel = RelationshipChangeBuilder()
                    .withProperties(deletedRelProperties.getOrDefault(it.id, emptyMap()))
                    .build()

            // start and end can be unreachable in case of detach delete
            val startNodeLabels = if  (txd.isDeleted(it.startNode)) builder.deletedLabels(it.startNode.id) else it.startNode.labelNames()
            val endNodeLabels = if  (txd.isDeleted(it.endNode)) builder.deletedLabels(it.endNode.id) else it.endNode.labelNames()

            val payload = RelationshipPayloadBuilder()
                    .withId(it.id.toString())
                    .withName(it.type.name())
                    .withStartNode(it.startNode.id.toString(), startNodeLabels)
                    .withEndNode(it.endNode.id.toString(), endNodeLabels)
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

    override fun afterRollback(p0: TransactionData?, p1: PreviousTransactionData?) {
    }

    override fun afterCommit(txd: TransactionData, previousTxd: PreviousTransactionData) {

        //FIXME schema

        val nodePrevious = previousTxd.nodeData
        val relPrevious = previousTxd.relData

        val totalEventsCount = nodePrevious.createdPayload.size + nodePrevious.deletedPayload.size + nodePrevious.updatedPayloads.size +
                relPrevious.createdPayload.size + relPrevious.deletedPayload.size + relPrevious.updatedPayloads.size
        var eventId = 0

        eventId = sendPayloads(OperationType.created, nodePrevious.createdPayload, txd, totalEventsCount, eventId)
        eventId = sendPayloads(OperationType.deleted, nodePrevious.deletedPayload, txd, totalEventsCount, eventId)
        eventId = sendPayloads(OperationType.updated, nodePrevious.updatedPayloads, txd, totalEventsCount, eventId)

        eventId = sendPayloads(OperationType.created, relPrevious.createdPayload, txd, totalEventsCount, eventId)
        eventId = sendPayloads(OperationType.deleted, relPrevious.deletedPayload, txd, totalEventsCount, eventId)
        eventId = sendPayloads(OperationType.updated, relPrevious.updatedPayloads, txd, totalEventsCount, eventId)

    }

    override fun beforeCommit(txd: TransactionData): PreviousTransactionData {

        var builder = PreviousTransactionDataBuilder()
        builder = buildNodeChanges(txd, builder)
        builder =  buildRelationshipChanges(txd, builder)

        return builder.build()
    }
}