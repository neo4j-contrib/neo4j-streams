package streams

import org.neo4j.graphdb.event.TransactionData
import org.neo4j.graphdb.event.TransactionEventHandler
import streams.events.StreamsEventBuilder
import streams.events.StreamsEventMetaBuilder

data class PreviousTransactionData(val labels : List<String>)

class StreamsTransactionEventHandler(val router : StreamsEventRouter) : TransactionEventHandler<PreviousTransactionData> {

    override fun afterCommit(txd: TransactionData, p1: PreviousTransactionData?) {

        val meta = StreamsEventMetaBuilder().withTimestamp(txd.commitTime).build()
        val builder = StreamsEventBuilder().withMeta(meta)
        val event = builder.build()
        router.sendEvent(event)
    }

    override fun beforeCommit(p0: TransactionData?): PreviousTransactionData {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun afterRollback(p0: TransactionData?, p1: PreviousTransactionData?) {
    }
}