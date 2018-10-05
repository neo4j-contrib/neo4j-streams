package streams.events

import org.neo4j.graphdb.Node
import org.neo4j.graphdb.event.PropertyEntry

data class PreviousTransactionData(val nodeProperties : Map<Long,Map<String,Any>>,
                                   val nodeLabels: Map<Long, List<String>>)

class PreviousTransactionDataBuilder(){

    private var nodeProperties : Map<Long,Map<String,Any>> = emptyMap()
    private var nodeLabels: Map<Long, List<String>> = emptyMap()

    //fun withAssignedNodeProperties()

    fun build() : PreviousTransactionData{
        return PreviousTransactionData(nodeProperties, nodeLabels)
    }

    fun withAssignedNodeProperties(assignedNodeProperties: MutableIterable<PropertyEntry<Node>>) : PreviousTransactionDataBuilder {
        TODO("implement the method")
        return this
    }
}