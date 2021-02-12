package streams

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.mockito.Mockito
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.event.LabelEntry
import org.neo4j.graphdb.event.PropertyEntry
import org.neo4j.graphdb.event.TransactionData
import org.neo4j.graphdb.schema.ConstraintDefinition
import org.neo4j.graphdb.schema.ConstraintType
import org.neo4j.graphdb.schema.Schema
import org.neo4j.kernel.internal.GraphDatabaseAPI
import streams.events.*
import streams.mocks.MockStreamsEventRouter
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

@Suppress("UNCHECKED_CAST")
class StreamsTransactionEventHandlerTest {

    private lateinit var handler: StreamsTransactionEventHandler
    private lateinit var schemaMock: Schema
    private lateinit var streamsConstraintsService: StreamsConstraintsService

    @Before
    fun setUp() {
        val dbMock = Mockito.mock(GraphDatabaseAPI::class.java)
        schemaMock = Mockito.mock(Schema::class.java)
        Mockito.`when`(schemaMock.getConstraints(Mockito.any(Label::class.java))).thenReturn(emptyList())
        Mockito.`when`(schemaMock.getConstraints(Mockito.any(RelationshipType::class.java))).thenReturn(emptyList())
        Mockito.`when`(dbMock.schema()).thenReturn(schemaMock)
        streamsConstraintsService = StreamsConstraintsService(dbMock, 0)
        streamsConstraintsService.start()
        handler = StreamsTransactionEventHandler(MockStreamsEventRouter(), dbMock,
                streamsConstraintsService)
        MockStreamsEventRouter.reset()
    }

    @After
    fun shutdown() {
        streamsConstraintsService.close()
    }

    @Test
    fun afterCreatedNodes() = runBlocking {

        val mockedNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedNode.id).thenReturn(1)
        Mockito.`when`(mockedNode.labels).thenReturn(listOf(Label.label("Person")))
        Mockito.`when`(mockedNode.allProperties).thenReturn(mapOf("name" to "Andrea", "surname" to "Santurbano"))

        val txd = Mockito.mock(TransactionData::class.java)
        Mockito.`when`(txd.createdNodes()).thenReturn(listOf(mockedNode))
        Mockito.`when`(txd.username()).thenReturn("mock")

        val constraintDefinition = Mockito.mock(ConstraintDefinition::class.java)
        Mockito.`when`(constraintDefinition.constraintType).thenReturn(ConstraintType.NODE_KEY)
        Mockito.`when`(constraintDefinition.label).thenReturn(Label.label("Person"))
        Mockito.`when`(constraintDefinition.relationshipType).thenThrow(IllegalStateException("Constraint is associated with nodes"))
        Mockito.`when`(constraintDefinition.propertyKeys).thenReturn(listOf("name", "surname"))

        Mockito.`when`(schemaMock.getConstraints(Label.label("Person"))).thenReturn(listOf(constraintDefinition))
        Mockito.`when`(schemaMock.constraints).thenReturn(listOf(constraintDefinition))

        delay(500) // wait the StreamsConstraintsService to load the constraints
        val previous = handler.beforeCommit(txd)
        handler.afterCommit(txd, previous)

        assertEquals(1, MockStreamsEventRouter.events.size)
        assertEquals(OperationType.created, MockStreamsEventRouter.events[0].meta.operation)
        assertNull(MockStreamsEventRouter.events[0].payload.before)
        assertNotNull(MockStreamsEventRouter.events[0].payload.after)
        assertEquals("1", MockStreamsEventRouter.events[0].payload.id)
        assertEquals(EntityType.node, MockStreamsEventRouter.events[0].payload.type)

        assertEquals(mapOf("name" to "String", "surname" to "String"), MockStreamsEventRouter.events[0].schema.properties)
        assertEquals(listOf(Constraint("Person", setOf("name", "surname"), StreamsConstraintType.UNIQUE)), MockStreamsEventRouter.events[0].schema.constraints)
    }

    @Test
    fun afterCreatedNodesWithLabel() {

        val mockedNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedNode.id).thenReturn(1)
        Mockito.`when`(mockedNode.labels).thenReturn(listOf(Label.label("Test")))

        val txd = Mockito.mock(TransactionData::class.java)
        Mockito.`when`(txd.createdNodes()).thenReturn(listOf(mockedNode))
        Mockito.`when`(txd.username()).thenReturn("mock")
        
        val previous = handler.beforeCommit(txd)
        handler.afterCommit(txd, previous)

        assertEquals(1, MockStreamsEventRouter.events.size)
        assertEquals(OperationType.created, MockStreamsEventRouter.events[0].meta.operation)
        assertNull(MockStreamsEventRouter.events[0].payload.before)
        assertNotNull(MockStreamsEventRouter.events[0].payload.after)
        assertEquals("1", MockStreamsEventRouter.events[0].payload.id)
        assertEquals(EntityType.node, MockStreamsEventRouter.events[0].payload.type)
        val after : NodeChange = MockStreamsEventRouter.events[0].payload.after as NodeChange
        assertEquals(listOf("Test"), after.labels)
    }

    @Test
    fun afterCreatedNodesWithProperties() {

        val mockedNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedNode.id).thenReturn(1)
        Mockito.`when`(mockedNode.allProperties).thenReturn(mapOf("name" to "Omar"))

        val txd = Mockito.mock(TransactionData::class.java)
        Mockito.`when`(txd.createdNodes()).thenReturn(listOf(mockedNode))
        Mockito.`when`(txd.username()).thenReturn("mock")

        val previous = handler.beforeCommit(txd)
        handler.afterCommit(txd, previous)

        assertEquals(1, MockStreamsEventRouter.events.size)
        assertEquals(OperationType.created, MockStreamsEventRouter.events[0].meta.operation)
        assertNull(MockStreamsEventRouter.events[0].payload.before)
        assertNotNull(MockStreamsEventRouter.events[0].payload.after)
        assertEquals("1", MockStreamsEventRouter.events[0].payload.id)
        assertEquals(EntityType.node, MockStreamsEventRouter.events[0].payload.type)
        val after : NodeChange = MockStreamsEventRouter.events[0].payload.after as NodeChange
        assertEquals("Omar",after.properties!!["name"])
    }

    @Test
    fun afterDeletedNodes() {

        val mockedNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedNode.id).thenReturn(1)

        val txd = Mockito.mock(TransactionData::class.java)
        Mockito.`when`(txd.deletedNodes()).thenReturn(listOf(mockedNode))
        Mockito.`when`(txd.username()).thenReturn("mock")

        val previous = handler.beforeCommit(txd)// PreviousTransactionData(nodeProperties = emptyMap(), nodeLabels = emptyMap(), createdPayload = emptyList(), deletedPayload = emptyList())
        handler.afterCommit(txd, previous)

        assertEquals(1, MockStreamsEventRouter.events.size)
        assertEquals(OperationType.deleted, MockStreamsEventRouter.events[0].meta.operation)
        assertNull(MockStreamsEventRouter.events[0].payload.after)
        assertNotNull(MockStreamsEventRouter.events[0].payload.before)
        assertEquals("1", MockStreamsEventRouter.events[0].payload.id)
        assertEquals(EntityType.node, MockStreamsEventRouter.events[0].payload.type)

    }

    @Test
    fun afterDeletedNodesWithLabel() {

        val mockedNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedNode.id).thenReturn(1)

        val labelEntry = Mockito.mock(LabelEntry::class.java)
        Mockito.`when`(labelEntry.label()).thenReturn(Label.label("Test"))
        Mockito.`when`(labelEntry.node()).thenReturn(mockedNode)

        val txd = Mockito.mock(TransactionData::class.java)
        Mockito.`when`(txd.deletedNodes()).thenReturn(listOf(mockedNode))
        Mockito.`when`(txd.removedLabels()).thenReturn(listOf(labelEntry))
        Mockito.`when`(txd.username()).thenReturn("mock")

        val previous = handler.beforeCommit(txd) // PreviousTransactionData(nodeProperties = emptyMap(), nodeLabels = labels, createdPayload = emptyList(), deletedPayload = emptyList())
        handler.afterCommit(txd, previous)

        assertEquals(1, MockStreamsEventRouter.events.size)
        assertEquals(OperationType.deleted, MockStreamsEventRouter.events[0].meta.operation)
        assertNull(MockStreamsEventRouter.events[0].payload.after)
        assertNotNull(MockStreamsEventRouter.events[0].payload.before)
        assertEquals("1", MockStreamsEventRouter.events[0].payload.id)
        assertEquals(EntityType.node, MockStreamsEventRouter.events[0].payload.type)
        val before : NodeChange = MockStreamsEventRouter.events[0].payload.before as NodeChange
        assertEquals(listOf("Test"), before.labels)
    }

    @Test
    fun afterDeletedNodesWithProperties() {

        val mockedNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedNode.id).thenReturn(1)

        val removedProp = Mockito.mock(PropertyEntry::class.java) as PropertyEntry<Node>
        Mockito.`when`(removedProp.entity()).thenReturn(mockedNode)
        Mockito.`when`(removedProp.key()).thenReturn("name")
        Mockito.`when`(removedProp.value()).thenReturn(null)
        Mockito.`when`(removedProp.previouslyCommitedValue()).thenReturn("Omar")

        val txd = Mockito.mock(TransactionData::class.java)
        Mockito.`when`(txd.deletedNodes()).thenReturn(listOf(mockedNode))
        Mockito.`when`(txd.removedNodeProperties()).thenReturn(listOf(removedProp))
        Mockito.`when`(txd.username()).thenReturn("mock")

        val previous = handler.beforeCommit(txd)//PreviousTransactionData(nodeProperties = mapOf("1" to props), nodeLabels = emptyMap(), createdPayload = emptyList(), deletedPayload = emptyList())
        handler.afterCommit(txd, previous)

        assertEquals(1, MockStreamsEventRouter.events.size)
        assertEquals(OperationType.deleted, MockStreamsEventRouter.events[0].meta.operation)
        assertNull(MockStreamsEventRouter.events[0].payload.after)
        assertNotNull(MockStreamsEventRouter.events[0].payload.before)
        assertEquals("1", MockStreamsEventRouter.events[0].payload.id)
        assertEquals(EntityType.node, MockStreamsEventRouter.events[0].payload.type)
        val before : NodeChange = MockStreamsEventRouter.events[0].payload.before as NodeChange
        assertEquals("Omar",before.properties!!["name"])
    }

    @Test
    fun afterUpdateLabelNodes() {

        val mockedNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedNode.id).thenReturn(1)
        Mockito.`when`(mockedNode.labels).thenReturn(listOf(Label.label("PreTest"), Label.label("Test")))

        val labelEntry = Mockito.mock(LabelEntry::class.java)
        Mockito.`when`(labelEntry.label()).thenReturn(Label.label("Test"))
        Mockito.`when`(labelEntry.node()).thenReturn(mockedNode)

        val txd = Mockito.mock(TransactionData::class.java)
        Mockito.`when`(txd.assignedLabels()).thenReturn(listOf(labelEntry))
        Mockito.`when`(txd.username()).thenReturn("mock")

        val previous = handler.beforeCommit(txd)
        handler.afterCommit(txd, previous)

        assertEquals(1, MockStreamsEventRouter.events.size)
        assertEquals(OperationType.updated, MockStreamsEventRouter.events[0].meta.operation)
        assertNotNull(MockStreamsEventRouter.events[0].payload.after)
        assertNotNull(MockStreamsEventRouter.events[0].payload.before)
        assertEquals("1", MockStreamsEventRouter.events[0].payload.id)
        assertEquals(EntityType.node, MockStreamsEventRouter.events[0].payload.type)

        val before : NodeChange = MockStreamsEventRouter.events[0].payload.before as NodeChange
        assertEquals(listOf("PreTest"),before.labels)

        val after : NodeChange = MockStreamsEventRouter.events[0].payload.after as NodeChange
        assertEquals(listOf("PreTest","Test"),after.labels)

    }

    @Test
    fun afterUpdatePropertiesNodes() {
        val prevProps = mapOf("name" to "Omar")
        val afterProps = mapOf("name" to "Andrea")

        val mockedNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedNode.id).thenReturn(1)
        Mockito.`when`(mockedNode.labels).thenReturn(listOf(Label.label("Test")))
        Mockito.`when`(mockedNode.allProperties).thenReturn(afterProps)

        val prop = Mockito.mock(PropertyEntry::class.java) as PropertyEntry<Node>
        Mockito.`when`(prop.entity()).thenReturn(mockedNode)
        Mockito.`when`(prop.key()).thenReturn("name")
        Mockito.`when`(prop.value()).thenReturn("Andrea")
        Mockito.`when`(prop.previouslyCommitedValue()).thenReturn("Omar")

        val txd = Mockito.mock(TransactionData::class.java)
        Mockito.`when`(txd.assignedNodeProperties()).thenReturn(listOf(prop))
        Mockito.`when`(txd.username()).thenReturn("mock")

        val previous = handler.beforeCommit(txd)
        handler.afterCommit(txd, previous)

        assertEquals(1, MockStreamsEventRouter.events.size)
        assertEquals(OperationType.updated, MockStreamsEventRouter.events[0].meta.operation)
        assertNotNull(MockStreamsEventRouter.events[0].payload.after)
        assertNotNull(MockStreamsEventRouter.events[0].payload.before)
        assertEquals("1", MockStreamsEventRouter.events[0].payload.id)
        assertEquals(EntityType.node, MockStreamsEventRouter.events[0].payload.type)

        val before : NodeChange = MockStreamsEventRouter.events[0].payload.before as NodeChange
        assertEquals(prevProps,before.properties)
        assertEquals(listOf("Test"),before.labels)

        val after : NodeChange = MockStreamsEventRouter.events[0].payload.after as NodeChange
        assertEquals(afterProps,after.properties)
        assertEquals(listOf("Test"),after.labels)

    }

    @Test
    fun beforeCommitAddLabel() {

        val mockedNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedNode.id).thenReturn(1)
        Mockito.`when`(mockedNode.labels).thenReturn(listOf(Label.label("PreTest"), Label.label("Test")))

        val labelEntry = Mockito.mock(LabelEntry::class.java)
        Mockito.`when`(labelEntry.label()).thenReturn(Label.label("Test"))
        Mockito.`when`(labelEntry.node()).thenReturn(mockedNode)

        val txd = Mockito.mock(TransactionData::class.java)
        Mockito.`when`(txd.assignedLabels()).thenReturn(listOf(labelEntry))
        Mockito.`when`(txd.username()).thenReturn("mock")

        val previous = handler.beforeCommit(txd).nodeData
        assertEquals(1, previous.nodeProperties.size)
        assertEquals(1, previous.nodeLabels.size)
        assertEquals(1, previous.nodeLabels[1]!!.size)
        assertEquals("PreTest", previous.nodeLabels[1]!![0])

        assertEquals(1, previous.updatedPayloads.size)
        assertEquals ("1", previous.updatedPayloads[0].id)
    }

    @Test
    fun beforeCommitRemoveLabel() {
        val mockedNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedNode.id).thenReturn(1)
        Mockito.`when`(mockedNode.labels).thenReturn(listOf(Label.label("PreTest")))

        val labelEntry = Mockito.mock(LabelEntry::class.java)
        Mockito.`when`(labelEntry.label()).thenReturn(Label.label("Test"))
        Mockito.`when`(labelEntry.node()).thenReturn(mockedNode)

        val txd = Mockito.mock(TransactionData::class.java)
        Mockito.`when`(txd.removedLabels()).thenReturn(listOf(labelEntry))
        Mockito.`when`(txd.username()).thenReturn("mock")

        val previous = handler.beforeCommit(txd).nodeData
        assertEquals(1, previous.nodeProperties.size)
        assertEquals(1, previous.nodeLabels.size)
        assertEquals(2, previous.nodeLabels[1]!!.size)
        assertEquals("PreTest", previous.nodeLabels[1]!![0])
        assertEquals("Test", previous.nodeLabels[1]!![1])

        assertEquals(1, previous.updatedPayloads.size)
        assertEquals ("1", previous.updatedPayloads[0].id)
    }

    @Test
    fun beforeCommitAddProperty() {

        val mockedNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedNode.id).thenReturn(1)

        val prop = Mockito.mock(PropertyEntry::class.java) as PropertyEntry<Node>
        Mockito.`when`(prop.entity()).thenReturn(mockedNode)
        Mockito.`when`(prop.key()).thenReturn("name")
        Mockito.`when`(prop.value()).thenReturn("value")
        Mockito.`when`(prop.previouslyCommitedValue()).thenReturn(null)

        val txd = Mockito.mock(TransactionData::class.java)
        Mockito.`when`(txd.assignedNodeProperties()).thenReturn(listOf(prop))
        Mockito.`when`(txd.username()).thenReturn("mock")

        val previous = handler.beforeCommit(txd).nodeData
        assertEquals(1, previous.nodeProperties.size)
        assertTrue { previous.nodeProperties[1]!!.isEmpty() }

        assertEquals(1, previous.updatedPayloads.size)
        assertEquals ("1", previous.updatedPayloads[0].id)
    }

    @Test
    fun beforeCommitRemoveProperty() {

        val mockedNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedNode.id).thenReturn(1)

        val prop = Mockito.mock(PropertyEntry::class.java) as PropertyEntry<Node>
        Mockito.`when`(prop.entity()).thenReturn(mockedNode)
        Mockito.`when`(prop.key()).thenReturn("p1")
        Mockito.`when`(prop.value()).thenReturn(null)
        Mockito.`when`(prop.previouslyCommitedValue()).thenReturn("value0")

        val txd = Mockito.mock(TransactionData::class.java)
        Mockito.`when`(txd.removedNodeProperties()).thenReturn(listOf(prop))
        Mockito.`when`(txd.username()).thenReturn("mock")

        val previous = handler.beforeCommit(txd).nodeData
        assertEquals(1, previous.nodeProperties.size)
        assertEquals("value0", previous.nodeProperties[1]!!["p1"])

        assertEquals(1, previous.updatedPayloads.size)
        assertEquals ("1", previous.updatedPayloads[0].id)

    }

    @Test
    fun beforeCommitSetProperty() {

        val mockedNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedNode.id).thenReturn(1)
        Mockito.`when`(mockedNode.allProperties).thenReturn(mapOf("p1" to "value1", "p2" to "value2", "p3" to "value4"))

        val prop1 = Mockito.mock(PropertyEntry::class.java) as PropertyEntry<Node>
        Mockito.`when`(prop1.entity()).thenReturn(mockedNode)
        Mockito.`when`(prop1.key()).thenReturn("p1")
        Mockito.`when`(prop1.value()).thenReturn("value1")
        Mockito.`when`(prop1.previouslyCommitedValue()).thenReturn("value0")

        val prop2 = Mockito.mock(PropertyEntry::class.java) as PropertyEntry<Node>
        Mockito.`when`(prop2.entity()).thenReturn(mockedNode)
        Mockito.`when`(prop2.key()).thenReturn("p3")
        Mockito.`when`(prop2.value()).thenReturn("value4")
        Mockito.`when`(prop2.previouslyCommitedValue()).thenReturn("value3")

        val txd = Mockito.mock(TransactionData::class.java)
        Mockito.`when`(txd.assignedNodeProperties()).thenReturn(listOf(prop1, prop2))
        Mockito.`when`(txd.username()).thenReturn("mock")

        val previous = handler.beforeCommit(txd).nodeData
        assertEquals(1, previous.nodeProperties.size)
        assertEquals("value0", previous.nodeProperties[1]!!["p1"])
        assertEquals("value2", previous.nodeProperties[1]!!["p2"])
        assertEquals("value3", previous.nodeProperties[1]!!["p3"])

        assertEquals(1, previous.updatedPayloads.size)
        assertEquals ("1", previous.updatedPayloads[0].id)
    }


    @Test
    fun beforeCommitMultinodes() {

        val mockedNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedNode.id).thenReturn(1)

        val mockedNodeLabels = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedNodeLabels.id).thenReturn(2)
        Mockito.`when`(mockedNodeLabels.labels).thenReturn(listOf(Label.label("PreTest"),Label.label("Test")))

        val prop1 = Mockito.mock(PropertyEntry::class.java) as PropertyEntry<Node>
        Mockito.`when`(prop1.entity()).thenReturn(mockedNode)
        Mockito.`when`(prop1.key()).thenReturn("p1")
        Mockito.`when`(prop1.value()).thenReturn("value1")
        Mockito.`when`(prop1.previouslyCommitedValue()).thenReturn("value0")

        val labelEntry = Mockito.mock(LabelEntry::class.java)
        Mockito.`when`(labelEntry.label()).thenReturn(Label.label("Test"))
        Mockito.`when`(labelEntry.node()).thenReturn(mockedNodeLabels)

        val txd = Mockito.mock(TransactionData::class.java)
        Mockito.`when`(txd.assignedNodeProperties()).thenReturn(listOf(prop1))
        Mockito.`when`(txd.assignedLabels()).thenReturn(listOf(labelEntry))
        Mockito.`when`(txd.username()).thenReturn("mock")

        val previous = handler.beforeCommit(txd).nodeData
        assertEquals(2, previous.nodeProperties.size)
        assertEquals("value0", previous.nodeProperties[1]!!["p1"])

        assertEquals(2, previous.updatedPayloads.size)
        assertEquals (1, previous.updatedPayloads.filter { it.id == "1" }.size)
        assertEquals (1, previous.updatedPayloads.filter { it.id == "2" }.size)
    }
}