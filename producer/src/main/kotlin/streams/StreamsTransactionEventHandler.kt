package streams

import org.neo4j.graphdb.event.TransactionData
import org.neo4j.graphdb.event.TransactionEventHandler
import streams.events.*
import java.net.InetAddress

class StreamsTransactionEventHandler(private val router : StreamsEventRouter) : TransactionEventHandler<PreviousTransactionData> {

    override fun afterCommit(txd: TransactionData, previous: PreviousTransactionData) {

        //FIXME refactor
        //FIXME schema

        val totalEventsCount = previous.createdPayload.size + previous.deletedPayload.size + previous.updatedPayloads.size
        var eventId = 0

        val createdEvents = previous.createdPayload.map { it ->
            val meta = StreamsEventMetaBuilder()
                    .withOperation(OperationType.created)
                    .withTransactionEventId(eventId++)
                    .withTransactionEventsCount(totalEventsCount)
                    .withUsername(txd.username())
                    .withTimestamp(txd.commitTime)
                    .withTransactionId(txd.transactionId)
                    .withHostname(InetAddress.getLocalHost().hostName)
                    .build()
            val schema = SchemaBuilder().build()

            val builder = StreamsEventBuilder()
                    .withMeta(meta)
                    .withPayload(it)
                    .withSchema(schema)

            val event = builder.build()
            event
        }

        val deletedEvents = previous.deletedPayload.map { it ->
            val meta = StreamsEventMetaBuilder()
                    .withOperation(OperationType.deleted)
                    .withTransactionEventId(eventId++)
                    .withTransactionEventsCount(totalEventsCount)
                    .withUsername(txd.username())
                    .withTimestamp(txd.commitTime)
                    .withTransactionId(txd.transactionId)
                    .withHostname(InetAddress.getLocalHost().hostName)
                    .build()

            val schema = SchemaBuilder().build()

            val builder = StreamsEventBuilder()
                    .withMeta(meta)
                    .withPayload(it)
                    .withSchema(schema)

            val event = builder.build()
            event
        }

        val updatedNodes = previous.updatedPayloads.map { it ->
            val meta = StreamsEventMetaBuilder()
                    .withOperation(OperationType.updated)
                    .withTransactionEventId(eventId++)
                    .withTransactionEventsCount(totalEventsCount)
                    .withUsername(txd.username())
                    .withTimestamp(txd.commitTime)
                    .withTransactionId(txd.transactionId)
                    .withHostname(InetAddress.getLocalHost().hostName)
                    .build()

            val schema = SchemaBuilder().build()

            val builder = StreamsEventBuilder()
                    .withMeta(meta)
                    .withPayload(it)
                    .withSchema(schema)

            val event = builder.build()
            event
        }

        createdEvents.forEach({ router.sendEvent(it)})
        deletedEvents.forEach({ router.sendEvent(it)})
        updatedNodes.forEach({ router.sendEvent(it)})
    }

    override fun beforeCommit(txd: TransactionData): PreviousTransactionData {

        val createdPayload = txd.createdNodes().map {
            val labels = it.labels.map { it.name() }

            val afterNode = NodeChangeBuilder()
                    .withLabels(labels)
                    .withProperites(it.allProperties)
                    .build()

            val payload = NodePayloadBuilder()
                    .withId(it.id.toString())
                    .withAfter(afterNode)
                    .build()

            payload
        }

        // labels and properties of deleted nodes are unreachable

        //FIXME refactor see PreviousTransactionDataBuilder
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
                    .withProperites(deletedNodeProperties.getOrDefault(it.id, emptyMap()))
                    .build()

            val payload = NodePayloadBuilder()
                    .withId(it.id.toString())
                    .withBefore(beforeNode)
                    .build()

            payload
        }

        //don't change the order of the with methods
        val builder = PreviousTransactionDataBuilder()
                .withLabels(txd.assignedLabels(),removedLabels)
                .withNodeProperties(txd.assignedNodeProperties(),removedNodeProperties)
                .withCreatedPayloads(createdPayload)
                .withDeletedPayloads(deletedPayload)

        return builder.build()
    }

    override fun afterRollback(p0: TransactionData?, p1: PreviousTransactionData?) {
    }
}