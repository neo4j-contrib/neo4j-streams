package streams

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.mockito.Mockito
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.event.PropertyEntry
import org.neo4j.graphdb.event.TransactionData
import org.neo4j.graphdb.schema.ConstraintDefinition
import org.neo4j.graphdb.schema.ConstraintType
import org.neo4j.graphdb.schema.Schema
import org.neo4j.kernel.internal.GraphDatabaseAPI
import streams.events.*
import streams.events.StreamsConstraintType
import streams.mocks.MockStreamsEventRouter
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

@Suppress("UNCHECKED_CAST")
class StreamsTransactionEventHandlerRelTest {

    private lateinit var handler: StreamsTransactionEventHandler
    private lateinit var schemaMock: Schema

    private lateinit var streamsConstraintsService: StreamsConstraintsService

    @Before
    fun setUp() {
        val dbMock = Mockito.mock(GraphDatabaseAPI::class.java)
        schemaMock = Mockito.mock(Schema::class.java)
        Mockito.`when`(schemaMock.getConstraints(Mockito.any(Label::class.java))).thenReturn(emptyList())
        Mockito.`when`(schemaMock.getConstraints(Mockito.any(RelationshipType::class.java))).thenReturn(emptyList())
        Mockito.`when`(schemaMock.constraints).thenReturn(emptyList())
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
    fun beforeCreatedRel() {
        val startLabel = "Start"
        val endLabel = "End"
        val relType = "REL"
        val relationshipProperties = mapOf("p1" to 1, "p2" to "2")
        val mockedRel = createMockedRelationship(startLabel, endLabel, relType, relationshipProperties)

        val txd = Mockito.mock(TransactionData::class.java)
        Mockito.`when`(txd.createdRelationships()).thenReturn(listOf(mockedRel))

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

    private fun createMockedRelationship(startLabel: String, endLabel: String, relType: String, relationshipProperties: Map<String, Any>): Relationship {
        val mockedStartNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedStartNode.id).thenReturn(1)
        Mockito.`when`(mockedStartNode.labels).thenReturn(listOf(Label.label(startLabel)))
        val mockedEndNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedEndNode.id).thenReturn(2)
        Mockito.`when`(mockedEndNode.labels).thenReturn(listOf(Label.label(endLabel)))
        val mockedRel = Mockito.mock(Relationship::class.java)
        Mockito.`when`(mockedRel.id).thenReturn(1)
        Mockito.`when`(mockedRel.type).thenReturn(RelationshipType.withName(relType))
        Mockito.`when`(mockedRel.startNode).thenReturn(mockedStartNode)
        Mockito.`when`(mockedRel.endNode).thenReturn(mockedEndNode)
        Mockito.`when`(mockedRel.allProperties).thenReturn(relationshipProperties)
        return mockedRel
    }

    @Test
    fun beforeDeleteRel() {
        val startLabel = "Start"
        val endLabel = "End"
        val relType = "REL"
        val relationshipProperties = emptyMap<String, Any>()
        val delRel = createMockedRelationship(startLabel, endLabel, relType, relationshipProperties)

        val removedPropP1 = Mockito.mock(PropertyEntry::class.java) as PropertyEntry<Relationship>
        Mockito.`when`(removedPropP1.entity()).thenReturn(delRel)
        Mockito.`when`(removedPropP1.key()).thenReturn("p1")
        Mockito.`when`(removedPropP1.value()).thenReturn(null)
        Mockito.`when`(removedPropP1.previouslyCommitedValue()).thenReturn(1)

        val removedPropP2 = Mockito.mock(PropertyEntry::class.java) as PropertyEntry<Relationship>
        Mockito.`when`(removedPropP2.entity()).thenReturn(delRel)
        Mockito.`when`(removedPropP2.key()).thenReturn("p2")
        Mockito.`when`(removedPropP2.value()).thenReturn(null)
        Mockito.`when`(removedPropP2.previouslyCommitedValue()).thenReturn("2")

        val txd = Mockito.mock(TransactionData::class.java)
        Mockito.`when`(txd.deletedRelationships()).thenReturn(listOf(delRel))
        Mockito.`when`(txd.removedRelationshipProperties()).thenReturn(mutableListOf(removedPropP1, removedPropP2))

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
    fun afterCreatedRel() = runBlocking {

        val startLabel = "Person"
        val endLabel = "City"
        val relType = "LIVES_IN"
        val relationshipProperties = mapOf("since" to 1984)
        val mockedStartNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedStartNode.id).thenReturn(1)
        Mockito.`when`(mockedStartNode.labels).thenReturn(listOf(Label.label(startLabel)))
        val startNodeProperties = mapOf("name" to "Andrea", "surname" to "Santurbano", "age" to 34)
        Mockito.`when`(mockedStartNode.allProperties).thenReturn(startNodeProperties)
        Mockito.`when`(mockedStartNode.propertyKeys).thenReturn(startNodeProperties.keys)
        Mockito.`when`(mockedStartNode.getProperties("name", "surname")).thenReturn(startNodeProperties.filterKeys { it == "name" || it == "surname" })
        val nodeStartConstraintDefinition = Mockito.mock(ConstraintDefinition::class.java)
        Mockito.`when`(nodeStartConstraintDefinition.constraintType).thenReturn(ConstraintType.NODE_KEY)
        Mockito.`when`(nodeStartConstraintDefinition.propertyKeys).thenReturn(listOf("name", "surname"))
        Mockito.`when`(nodeStartConstraintDefinition.relationshipType).thenThrow(IllegalStateException("Constraint is associated with nodes"))
        Mockito.`when`(nodeStartConstraintDefinition.label).thenReturn(Label.label("Person"))

        val mockedEndNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedEndNode.id).thenReturn(2)
        Mockito.`when`(mockedEndNode.labels).thenReturn(listOf(Label.label(endLabel)))
        val endNodeProperties = mapOf("name" to "Pescara", "postal_code" to "65100")
        Mockito.`when`(mockedEndNode.allProperties).thenReturn(endNodeProperties)
        Mockito.`when`(mockedEndNode.propertyKeys).thenReturn(endNodeProperties.keys)
        Mockito.`when`(mockedEndNode.getProperties("name")).thenReturn(endNodeProperties.filterKeys { it == "name" })
        val nodeEndConstraintDefinition = Mockito.mock(ConstraintDefinition::class.java)
        Mockito.`when`(nodeEndConstraintDefinition.constraintType).thenReturn(ConstraintType.UNIQUENESS)
        Mockito.`when`(nodeEndConstraintDefinition.propertyKeys).thenReturn(listOf("name"))
        Mockito.`when`(nodeEndConstraintDefinition.relationshipType).thenThrow(IllegalStateException("Constraint is associated with nodes"))
        Mockito.`when`(nodeEndConstraintDefinition.label).thenReturn(Label.label("City"))

        val mockedRel = Mockito.mock(Relationship::class.java)
        Mockito.`when`(mockedRel.id).thenReturn(1)
        Mockito.`when`(mockedRel.type).thenReturn(RelationshipType.withName(relType))
        Mockito.`when`(mockedRel.startNode).thenReturn(mockedStartNode)
        Mockito.`when`(mockedRel.endNode).thenReturn(mockedEndNode)
        Mockito.`when`(mockedRel.allProperties).thenReturn(relationshipProperties)

        val txd = Mockito.mock(TransactionData::class.java)
        Mockito.`when`(txd.createdRelationships()).thenReturn(listOf(mockedRel))
        Mockito.`when`(txd.username()).thenReturn("mock")

        val constraintDefinition = Mockito.mock(ConstraintDefinition::class.java)
        Mockito.`when`(constraintDefinition.constraintType).thenReturn(ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE)
        Mockito.`when`(constraintDefinition.relationshipType).thenReturn(RelationshipType.withName(relType))
        Mockito.`when`(constraintDefinition.propertyKeys).thenReturn(listOf("since"))
        Mockito.`when`(constraintDefinition.label).thenThrow(IllegalStateException("Constraint is associated with relationships"))

        Mockito.`when`(schemaMock.getConstraints(RelationshipType.withName(relType))).thenReturn(listOf(constraintDefinition))
        Mockito.`when`(schemaMock.getConstraints(Label.label(startLabel))).thenReturn(listOf(nodeStartConstraintDefinition))
        Mockito.`when`(schemaMock.getConstraints(Label.label(endLabel))).thenReturn(listOf(nodeEndConstraintDefinition))
        Mockito.`when`(schemaMock.constraints).thenReturn(listOf(nodeStartConstraintDefinition, nodeEndConstraintDefinition, constraintDefinition))

        delay(500) // wait the StreamsConstraintsService to load the constraints
        val prev = handler.beforeCommit(txd)
        handler.afterCommit(txd, prev)

        assertEquals(1, MockStreamsEventRouter.events.size)
        assertEquals(OperationType.created, MockStreamsEventRouter.events[0].meta.operation)
        assertNull(MockStreamsEventRouter.events[0].payload.before)
        assertNotNull(MockStreamsEventRouter.events[0].payload.after)

        val relPayload = MockStreamsEventRouter.events[0].payload as RelationshipPayload
        assertEquals(mapOf("name" to "Andrea", "surname" to "Santurbano"), relPayload.start.ids)
        assertEquals(mapOf("name" to "Pescara"), relPayload.end.ids)

        assertEquals("1", MockStreamsEventRouter.events[0].payload.id)
        assertEquals(EntityType.relationship, MockStreamsEventRouter.events[0].payload.type)
        assertEquals(mapOf("since" to "Integer"), MockStreamsEventRouter.events[0].schema.properties)

        val relConstraint = MockStreamsEventRouter.events[0].schema.constraints[0]
        assertEquals(Constraint(relType, setOf("since"), StreamsConstraintType.RELATIONSHIP_PROPERTY_EXISTS), relConstraint)
    }

    @Test
    fun afterDeletedRel() {

        val startLabel = "Start"
        val endLabel = "End"
        val relType = "REL"
        val relationshipProperties = emptyMap<String, Any>()
        val delRel = createMockedRelationship(startLabel, endLabel, relType, relationshipProperties)

        val removedPropP1 = Mockito.mock(PropertyEntry::class.java) as PropertyEntry<Relationship>
        Mockito.`when`(removedPropP1.entity()).thenReturn(delRel)
        Mockito.`when`(removedPropP1.key()).thenReturn("p1")
        Mockito.`when`(removedPropP1.value()).thenReturn(null)
        Mockito.`when`(removedPropP1.previouslyCommitedValue()).thenReturn(1)

        val removedPropP2 = Mockito.mock(PropertyEntry::class.java) as PropertyEntry<Relationship>
        Mockito.`when`(removedPropP2.entity()).thenReturn(delRel)
        Mockito.`when`(removedPropP2.key()).thenReturn("p2")
        Mockito.`when`(removedPropP2.value()).thenReturn(null)
        Mockito.`when`(removedPropP2.previouslyCommitedValue()).thenReturn("2")

        val txd = Mockito.mock(TransactionData::class.java)
        Mockito.`when`(txd.deletedRelationships()).thenReturn(listOf(delRel))
        Mockito.`when`(txd.removedRelationshipProperties()).thenReturn(mutableListOf(removedPropP1, removedPropP2))
        Mockito.`when`(txd.username()).thenReturn("mock")

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

        val startLabel = "Start"
        val endLabel = "End"
        val relType = "REL"
        val relationshipProperties = mapOf("p1" to 1, "p2" to "2")
        val mockedRel = createMockedRelationship(startLabel, endLabel, relType, relationshipProperties)

        val removedProp = Mockito.mock(PropertyEntry::class.java) as PropertyEntry<Relationship>
        Mockito.`when`(removedProp.entity()).thenReturn(mockedRel)
        Mockito.`when`(removedProp.key()).thenReturn("p3")
        Mockito.`when`(removedProp.value()).thenReturn(null)
        Mockito.`when`(removedProp.previouslyCommitedValue()).thenReturn(3)

        val assignedPropP1 = Mockito.mock(PropertyEntry::class.java) as PropertyEntry<Relationship>
        Mockito.`when`(assignedPropP1.entity()).thenReturn(mockedRel)
        Mockito.`when`(assignedPropP1.key()).thenReturn("p1")
        Mockito.`when`(assignedPropP1.value()).thenReturn(1)
        Mockito.`when`(assignedPropP1.previouslyCommitedValue()).thenReturn(null)

        val assignedPropP2 = Mockito.mock(PropertyEntry::class.java) as PropertyEntry<Relationship>
        Mockito.`when`(assignedPropP2.entity()).thenReturn(mockedRel)
        Mockito.`when`(assignedPropP2.key()).thenReturn("p2")
        Mockito.`when`(assignedPropP2.value()).thenReturn("2")
        Mockito.`when`(assignedPropP2.previouslyCommitedValue()).thenReturn("noval")

        val txd = Mockito.mock(TransactionData::class.java)
        Mockito.`when`(txd.removedRelationshipProperties()).thenReturn(mutableListOf(removedProp))
        Mockito.`when`(txd.assignedRelationshipProperties()).thenReturn(mutableListOf(assignedPropP1, assignedPropP2))
        Mockito.`when`(txd.username()).thenReturn("mock")

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

    @Test
    fun beforeUpdateRelWithDiffNodeConstraint() = runBlocking {

        val startLabel = "Person"
        val endLabel = "City"
        val relType = "LIVES_IN"
        val relationshipProperties = mapOf("since" to 2016)
        val mockedStartNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedStartNode.id).thenReturn(1)
        Mockito.`when`(mockedStartNode.labels).thenReturn(listOf(Label.label(startLabel)))
        val startNodeProperties = mapOf("name" to "James", "surname" to "Chu", "age" to 25)
        Mockito.`when`(mockedStartNode.allProperties).thenReturn(startNodeProperties)
        Mockito.`when`(mockedStartNode.propertyKeys).thenReturn(startNodeProperties.keys)
        Mockito.`when`(mockedStartNode.getProperties("name", "surname")).thenReturn(startNodeProperties.filterKeys { it == "name" || it == "surname" })
        val nodeStartConstraintDefinition = Mockito.mock(ConstraintDefinition::class.java)
        Mockito.`when`(nodeStartConstraintDefinition.constraintType).thenReturn(ConstraintType.NODE_KEY)
        Mockito.`when`(nodeStartConstraintDefinition.propertyKeys).thenReturn(listOf("name", "surname"))
        Mockito.`when`(nodeStartConstraintDefinition.relationshipType).thenThrow(IllegalStateException("Constraint is associated with nodes"))
        Mockito.`when`(nodeStartConstraintDefinition.label).thenReturn(Label.label("Person"))

        val mockedEndNode = Mockito.mock(Node::class.java)
        Mockito.`when`(mockedEndNode.id).thenReturn(2)
        Mockito.`when`(mockedEndNode.labels).thenReturn(listOf(Label.label(endLabel)))
        val endNodeProperties = mapOf("name" to "Beijing", "postal_code" to "100000")
        Mockito.`when`(mockedEndNode.allProperties).thenReturn(endNodeProperties)
        Mockito.`when`(mockedEndNode.propertyKeys).thenReturn(endNodeProperties.keys)
        Mockito.`when`(mockedEndNode.getProperties("name")).thenReturn(endNodeProperties.filterKeys { it == "name" })
        val nodeEndConstraintDefinition = Mockito.mock(ConstraintDefinition::class.java)
        Mockito.`when`(nodeEndConstraintDefinition.constraintType).thenReturn(ConstraintType.UNIQUENESS)
        Mockito.`when`(nodeEndConstraintDefinition.propertyKeys).thenReturn(listOf("name"))
        Mockito.`when`(nodeEndConstraintDefinition.relationshipType).thenThrow(IllegalStateException("Constraint is associated with nodes"))
        Mockito.`when`(nodeEndConstraintDefinition.label).thenReturn(Label.label("City"))

        val mockedRel = Mockito.mock(Relationship::class.java)
        Mockito.`when`(mockedRel.id).thenReturn(1)
        Mockito.`when`(mockedRel.type).thenReturn(RelationshipType.withName(relType))
        Mockito.`when`(mockedRel.startNode).thenReturn(mockedStartNode)
        Mockito.`when`(mockedRel.endNode).thenReturn(mockedEndNode)
        Mockito.`when`(mockedRel.allProperties).thenReturn(relationshipProperties)

        val constraintDefinition = Mockito.mock(ConstraintDefinition::class.java)
        Mockito.`when`(constraintDefinition.constraintType).thenReturn(ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE)
        Mockito.`when`(constraintDefinition.relationshipType).thenReturn(RelationshipType.withName(relType))
        Mockito.`when`(constraintDefinition.propertyKeys).thenReturn(listOf("since"))
        Mockito.`when`(constraintDefinition.label).thenThrow(IllegalStateException("Constraint is associated with relationships"))

        Mockito.`when`(schemaMock.getConstraints(RelationshipType.withName(relType))).thenReturn(listOf(constraintDefinition))
        Mockito.`when`(schemaMock.getConstraints(Label.label(startLabel))).thenReturn(listOf(nodeStartConstraintDefinition))
        Mockito.`when`(schemaMock.getConstraints(Label.label(endLabel))).thenReturn(listOf(nodeEndConstraintDefinition))
        Mockito.`when`(schemaMock.constraints).thenReturn(listOf(nodeStartConstraintDefinition, nodeEndConstraintDefinition, constraintDefinition))

        delay(500) // wait the StreamsConstraintsService to load the constraints

        val assignedProp = Mockito.mock(PropertyEntry::class.java) as PropertyEntry<Relationship>
        Mockito.`when`(assignedProp.entity()).thenReturn(mockedRel)
        Mockito.`when`(assignedProp.key()).thenReturn("since")
        Mockito.`when`(assignedProp.value()).thenReturn(2016)
        Mockito.`when`(assignedProp.previouslyCommitedValue()).thenReturn(2008)

        val txd = Mockito.mock(TransactionData::class.java)
        Mockito.`when`(txd.removedRelationshipProperties()).thenReturn(mutableListOf())
        Mockito.`when`(txd.assignedRelationshipProperties()).thenReturn(mutableListOf(assignedProp))
        Mockito.`when`(txd.username()).thenReturn("mock")

        val previous = handler.beforeCommit(txd).relData

        assertEquals(1,previous.updatedPayloads.size)

        val rel = previous.updatedPayloads[0]
        assertEquals("1",rel.id)
        assertEquals("LIVES_IN",rel.label)
        assertNotNull(rel.before)
        assertNotNull(rel.after)

        assertEquals("1", rel.start.id)
        assertEquals(mapOf("name" to "James", "surname" to "Chu"), rel.start.ids)
        assertEquals(listOf("Person"), rel.start.labels)
        assertEquals("2", rel.end.id)
        assertEquals(mapOf("name" to "Beijing"), rel.end.ids)
        assertEquals(listOf("City"), rel.end.labels)

        val after : RelationshipChange = rel.after as RelationshipChange
        assertEquals(mapOf("since" to 2016),after.properties)
        val before : RelationshipChange = rel.before as RelationshipChange
        assertEquals(mapOf("since" to 2008),before.properties)

        assertEquals(0,previous.deletedPayload.size)
        assertEquals(0,previous.createdPayload.size)
    }
}