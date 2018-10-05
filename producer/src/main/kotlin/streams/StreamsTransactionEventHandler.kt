package streams

import org.neo4j.graphdb.event.TransactionData
import org.neo4j.graphdb.event.TransactionEventHandler
import streams.events.PreviousTransactionData
import streams.events.PreviousTransactionDataBuilder
import streams.events.StreamsEventBuilder
import streams.events.StreamsEventMetaBuilder

class StreamsTransactionEventHandler(val router : StreamsEventRouter) : TransactionEventHandler<PreviousTransactionData> {

    override fun afterCommit(txd: TransactionData, p1: PreviousTransactionData?) {

        val meta = StreamsEventMetaBuilder().withTimestamp(txd.commitTime).build()
        val builder = StreamsEventBuilder().withMeta(meta)
        val event = builder.build()
        router.sendEvent(event)
    }

    override fun beforeCommit(txd: TransactionData): PreviousTransactionData {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        //txd.assignedNodeProperties().map { p -> p.previouslyCommitedValue() }
        val builder = PreviousTransactionDataBuilder()
        builder = builder.withAssignedNodeProperties(txd.assignedNodeProperties())

        return builder.build()
    }

    override fun afterRollback(p0: TransactionData?, p1: PreviousTransactionData?) {
    }
}