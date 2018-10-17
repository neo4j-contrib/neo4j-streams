package streams

import org.junit.Before
import org.junit.Test
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.event.PropertyEntry
import streams.events.EntityType
import streams.events.OperationType
import streams.events.RelationshipChange
import streams.mocks.*
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class StreamsTransactionEventHandlerRelTest {

    private val handler : StreamsTransactionEventHandler = StreamsTransactionEventHandler(MockStreamsEventRouter())


    @Before
    fun setUp() {
        MockStreamsEventRouter.reset()
    }

    @Test
    fun beforeCreatedRel() {

        val createdRels = mutableListOf<MockRelationship>(MockRelationship(
                id=1,
                type="REL",
                startNode=MockNode(nodeId = 1, labels = mutableListOf(Label.label("Start"))),
                endNode = MockNode(nodeId = 2, labels = mutableListOf(Label.label("End"))),
                properties = mutableMapOf("p1" to 1, "p2" to "2")))
        val txd = MockTransactionData(createdRelationships = createdRels)
        val previous = handler.beforeCommit(txd).relData

        assertEquals(1,previous.createdPayload.size)
        val rel = previous.createdPayload[0]
        assertEquals("1",rel.id)
        assertEquals("REL",rel.label)
        assertNull(rel.before)
        assertNotNull(rel.after)
        val after : RelationshipChange = rel.after as RelationshipChange
        assertEquals(mapOf("p1" to 1, "p2" to "2"),after.properties)
        assertEquals("1", rel.start.id)
        assertEquals(listOf("Start"), rel.start.labels)
        assertEquals("2", rel.end.id)
        assertEquals(listOf("End"), rel.end.labels)

        assertEquals(0,previous.deletedPayload.size)
        assertEquals(0,previous.updatedPayloads.size)
    }

    @Test
    fun beforeDeleteRel() {

        val delRel = MockRelationship(
                id=1,
                type="REL",
                startNode=MockNode(nodeId = 1, labels = mutableListOf(Label.label("Start"))),
                endNode = MockNode(nodeId = 2, labels = mutableListOf(Label.label("End"))))
        val removedProps = mutableListOf<PropertyEntry<Relationship>>(MockPropertyEntry<Relationship>( delRel,"p1", null, 1),
                MockPropertyEntry<Relationship>( delRel,"p2", null, "2"))

        val delRels = mutableListOf<MockRelationship>(delRel)
        val txd = MockTransactionData(deletedRelationships = delRels, removedRelationshipProperties = removedProps)
        val previous = handler.beforeCommit(txd).relData

        assertEquals(0,previous.createdPayload.size)
        assertEquals(1,previous.deletedPayload.size)
        assertEquals(0,previous.updatedPayloads.size)

        val rel = previous.deletedPayload[0]
        assertEquals("1",rel.id)
        assertEquals("REL",rel.label)
        assertNotNull(rel.before)
        assertNull(rel.after)
        val before : RelationshipChange = rel.before as RelationshipChange
        assertEquals(mapOf("p1" to 1, "p2" to "2"),before.properties)
        assertEquals("1", rel.start.id)
        assertEquals(listOf("Start"), rel.start.labels)
        assertEquals("2", rel.end.id)
        assertEquals(listOf("End"), rel.end.labels)

    }

    @Test
    fun afterCreatedRel() {

        val createdRels = mutableListOf<MockRelationship>(MockRelationship(
                id = 1,
                type = "REL",
                startNode = MockNode(nodeId = 1, labels = mutableListOf(Label.label("Start"))),
                endNode = MockNode(nodeId = 2, labels = mutableListOf(Label.label("End"))),
                properties = mutableMapOf("p1" to 1, "p2" to "2")))
        val txd = MockTransactionData(createdRelationships = createdRels)
        val prev = handler.beforeCommit(txd)
        handler.afterCommit(txd, prev)

        assertEquals(1, MockStreamsEventRouter.events.size)
        assertEquals(OperationType.created, MockStreamsEventRouter.events[0].meta.operation)
        assertNull(MockStreamsEventRouter.events[0].payload.before)
        assertNotNull(MockStreamsEventRouter.events[0].payload.after)
        assertEquals("1", MockStreamsEventRouter.events[0].payload.id)
        assertEquals(EntityType.relationship, MockStreamsEventRouter.events[0].payload.type)
    }

    @Test
    fun afterDeletedRel() {

        val delRel = MockRelationship(
                id=1,
                type="REL",
                startNode=MockNode(nodeId = 1, labels = mutableListOf(Label.label("Start"))),
                endNode = MockNode(nodeId = 2, labels = mutableListOf(Label.label("End"))))
        val removedProps = mutableListOf<PropertyEntry<Relationship>>(MockPropertyEntry<Relationship>( delRel,"p1", null, 1),
                MockPropertyEntry<Relationship>( delRel,"p2", null, "2"))

        val delRels = mutableListOf<MockRelationship>(delRel)
        val txd = MockTransactionData(deletedRelationships = delRels, removedRelationshipProperties = removedProps)
        val prev = handler.beforeCommit(txd)
        handler.afterCommit(txd, prev)

        assertEquals(1, MockStreamsEventRouter.events.size)
        assertEquals(OperationType.deleted, MockStreamsEventRouter.events[0].meta.operation)
        assertNotNull(MockStreamsEventRouter.events[0].payload.before)
        assertNull(MockStreamsEventRouter.events[0].payload.after)
        assertEquals("1", MockStreamsEventRouter.events[0].payload.id)
        assertEquals(EntityType.relationship, MockStreamsEventRouter.events[0].payload.type)
    }

    @Test
    fun beforeUpdateRel() {

        val relation = MockRelationship(
                id=1,
                type="REL",
                startNode=MockNode(nodeId = 1, labels = mutableListOf(Label.label("Start"))),
                endNode = MockNode(nodeId = 2, labels = mutableListOf(Label.label("End"))),
                properties = mutableMapOf("p1" to 1, "p2" to "2"))

        val createdRels = mutableListOf<MockRelationship>(relation)

        val removedProps = mutableListOf<PropertyEntry<Relationship>>(MockPropertyEntry<Relationship>( relation,"p3", null, 3))
        val assignedProps = mutableListOf<PropertyEntry<Relationship>>(MockPropertyEntry<Relationship>( relation,"p1", 1, null),
            MockPropertyEntry<Relationship>( relation,"p2", "2", "noval"))

        val txd = MockTransactionData(removedRelationshipProperties = removedProps, assignedRelationshipProperties = assignedProps)
        val previous = handler.beforeCommit(txd).relData

        assertEquals(1,previous.updatedPayloads.size)

        val rel = previous.updatedPayloads[0]
        assertEquals("1",rel.id)
        assertEquals("REL",rel.label)
        assertNotNull(rel.before)
        assertNotNull(rel.after)

        val after : RelationshipChange = rel.after as RelationshipChange
        assertEquals(mapOf("p1" to 1, "p2" to "2"),after.properties)
        assertEquals("1", rel.start.id)
        assertEquals(listOf("Start"), rel.start.labels)
        assertEquals("2", rel.end.id)
        assertEquals(listOf("End"), rel.end.labels)

        val before : RelationshipChange = rel.before as RelationshipChange
        assertEquals(mapOf("p2" to "noval", "p3" to 3),before.properties)
        assertEquals("1", rel.start.id)
        assertEquals(listOf("Start"), rel.start.labels)
        assertEquals("2", rel.end.id)
        assertEquals(listOf("End"), rel.end.labels)

        assertEquals(0,previous.deletedPayload.size)
        assertEquals(0,previous.createdPayload.size)
    }
}