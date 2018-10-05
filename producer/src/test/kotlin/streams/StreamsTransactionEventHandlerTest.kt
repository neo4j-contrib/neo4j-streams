package streams

import org.junit.Test
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.event.LabelEntry
import org.neo4j.graphdb.event.PropertyEntry
import streams.mocks.*
import kotlin.test.assertEquals

class StreamsTransactionEventHandlerTest {

    val router : MockStreamsEventRouter = MockStreamsEventRouter()
    val handler : StreamsTransactionEventHandler = StreamsTransactionEventHandler(router)

    @Test
    fun afterCommit() {
    }

    @Test
    fun beforeCommitAddLabel() {
        val labels = mutableListOf<LabelEntry>(MockLabelEntry(
                Label.label("Test"),
                MockNode(nodeId = 1, labels = mutableListOf(Label.label("PreTest")))))

        val txd = MockTransactionData(assignedLabels = labels)
        val previous = handler.beforeCommit(txd)
        assertEquals(0, previous.nodeProperties.size)
        assertEquals(1, previous.nodeLabels.size)
        assertEquals(1, previous.nodeLabels[1]!!.size)
        assertEquals("PreTest", previous.nodeLabels[1]!![0])
    }

    @Test
    fun beforeCommitRemoveLabel() {
        val labels = mutableListOf<LabelEntry>(MockLabelEntry(
                Label.label("Test"),
                MockNode(nodeId = 1, labels = mutableListOf(Label.label("PreTest")))))

        val txd = MockTransactionData(removedLabels = labels)
        val previous = handler.beforeCommit(txd)
        assertEquals(0, previous.nodeProperties.size)
        assertEquals(1, previous.nodeLabels.size)
        assertEquals(2, previous.nodeLabels[1]!!.size)
        assertEquals("PreTest", previous.nodeLabels[1]!![0])
        assertEquals("Test", previous.nodeLabels[1]!![1])
    }

    @Test
    fun beforeCommitAddProperty() {
        //FIXME check if it's correct
        val props = mutableListOf<PropertyEntry<Node>>()
        val node = MockNode()
        props.add(MockPropertyEntry<Node>(node, "p1", "value", null))
        val txd = MockTransactionData(assignedNodeProperties = props)
        val previous = handler.beforeCommit(txd)
        assertEquals(0, previous.nodeProperties.size)
    }

    @Test
    fun beforeCommitRemoveProperty() {
        //FIXME check if it's correct
        val props = mutableListOf<PropertyEntry<Node>>()
        val node = MockNode(nodeId = 1)
        props.add(MockPropertyEntry<Node>(node, "p1", "value0", "value0"))
        val txd = MockTransactionData(removedNodeProperties = props)
        val previous = handler.beforeCommit(txd)
        assertEquals(1, previous.nodeProperties.size)
        assertEquals("value0", previous.nodeProperties[1]!!["p1"])
    }

    @Test
    fun beforeCommitSetProperty() {
        val props = mutableListOf<PropertyEntry<Node>>()
        val node = MockNode(nodeId = 1)
        props.add(MockPropertyEntry<Node>(node, "p1", "value1", "value0"))
        val txd = MockTransactionData(assignedNodeProperties = props)
        val previous = handler.beforeCommit(txd)
        assertEquals(1, previous.nodeProperties.size)
        assertEquals("value0", previous.nodeProperties[1]!!["p1"])
    }


}