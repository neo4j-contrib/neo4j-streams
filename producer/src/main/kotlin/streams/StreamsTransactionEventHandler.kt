package streams

import org.neo4j.graphdb.event.TransactionData
import org.neo4j.graphdb.event.TransactionEventHandler
import streams.events.*

class StreamsTransactionEventHandler(val router : StreamsEventRouter) : TransactionEventHandler<PreviousTransactionData> {

    override fun afterCommit(txd: TransactionData, previous: PreviousTransactionData) {

        //FIXME implements with real data
        val meta = StreamsEventMetaBuilder()
                .withOperation(OperationType.updated)
                .withTransactionEventId(0)
                .withTransactionEventsCount(1)
                .withUsername("test")
                .withTimestamp(txd.commitTime)
                .withTransactionId(0)
                .build()

        val payload = NodePayloadBuilder().build()

        val schema = SchemaBuilder().build()

        val builder = StreamsEventBuilder()
                .withMeta(meta)
                .withPayload(payload)
                .withSchema(schema)

        val event = builder.build()

        previous.updatedNodeIds.forEach({ router.sendEvent(event) })
    }

    override fun beforeCommit(txd: TransactionData): PreviousTransactionData {
        val builder = PreviousTransactionDataBuilder()
                .withNodeProperties(txd.assignedNodeProperties(),txd.removedNodeProperties())
                .withLabels(txd.assignedLabels(),txd.removedLabels())

        return builder.build()
    }

    override fun afterRollback(p0: TransactionData?, p1: PreviousTransactionData?) {
    }
}