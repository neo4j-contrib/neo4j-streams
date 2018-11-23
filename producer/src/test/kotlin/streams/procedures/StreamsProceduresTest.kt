package streams.procedures

import org.junit.BeforeClass
import org.junit.Test
import org.neo4j.graphdb.Label
import streams.events.RelationshipNodeChange
import streams.labelNames
import streams.mocks.*
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import streams.NodeRoutingConfiguration
import streams.RelationshipRoutingConfiguration

class StreamsProceduresTest {

    companion object {
        lateinit var streamsProcedures: StreamsProcedures

        @BeforeClass
        @JvmStatic
        fun setup() {
            streamsProcedures = StreamsProcedures()
        }

    }

    @Test
    fun shouldCreateSimpleTypes() {
        streamsProcedures.db = MockGraphDatabaseAPI()
        val str = "Test"
        val strExpected = streamsProcedures.buildPayload("neo4j", str)
        assertEquals(strExpected, str)
    }

    @Test
    fun shouldCreateNode() {
        // Given
        streamsProcedures.db = MockGraphDatabaseAPI()
        val node = MockNode(id = 1, labels = mutableListOf(Label.label("Foo"), Label.label("Bar")),
                properties = mutableMapOf("prop" to "foo", "prop1" to "bar"))

        // When
        val nodePayload = streamsProcedures.buildPayload("neo4j", node)

        // Then
        assertTrue { nodePayload is Map<*, *> }
        val nodeMap = nodePayload as Map<String, Any>
        assertEquals(node.id.toString(), nodeMap["id"])
        assertEquals(node.labelNames(), nodeMap["labels"])
        assertEquals(node.properties, nodeMap["properties"])
    }

    @Test
    fun shouldCreateNodeWithIncludedProperties() {
        // Given
        val nodeRouting = NodeRoutingConfiguration(all = false, labels = listOf("Foo"), include = listOf("prop1"))
        val dependencyResolver = MockDependencyResolver(nodeRouting = listOf(nodeRouting))
        streamsProcedures.db = MockGraphDatabaseAPI(dependencyResolver)
        val node = MockNode(id = 1, labels = mutableListOf(Label.label("Foo"), Label.label("Bar")),
                properties = mutableMapOf("prop" to "foo", "prop1" to "bar"))

        // When
        val nodePayload = streamsProcedures.buildPayload("neo4j", node)

        // Then
        assertTrue { nodePayload is Map<*, *> }
        val nodeMap = nodePayload as Map<String, Any>
        assertEquals(node.id.toString(), nodeMap["id"])
        assertEquals(node.labelNames(), nodeMap["labels"])
        assertEquals(node.properties.filter { nodeRouting.include.contains(it.key) }, nodeMap["properties"])
    }

    @Test
    fun shouldCreateNodeWithoutExcludedProperties() {
        // Given
        val nodeRouting = NodeRoutingConfiguration(all = false, labels = listOf("Foo"), exclude = listOf("prop1"))
        val dependencyResolver = MockDependencyResolver(nodeRouting = listOf(nodeRouting))
        streamsProcedures.db = MockGraphDatabaseAPI(dependencyResolver)
        val node = MockNode(id = 1, labels = mutableListOf(Label.label("Foo"), Label.label("Bar")),
                properties = mutableMapOf("prop" to "foo", "prop1" to "bar"))

        // When
        val nodePayload = streamsProcedures.buildPayload("neo4j", node)

        // Then
        assertTrue { nodePayload is Map<*, *> }
        val nodeMap = nodePayload as Map<String, Any>
        assertEquals(node.id.toString(), nodeMap["id"])
        assertEquals(node.labelNames(), nodeMap["labels"])
        assertEquals(node.properties.filter { !nodeRouting.exclude.contains(it.key) }, nodeMap["properties"])
    }

    @Test
    fun shouldCreateRelationship() {
        // Given
        streamsProcedures.db = MockGraphDatabaseAPI()
        val relationship = MockRelationship(id = 10, type = "KNOWS", properties = mutableMapOf("prop" to "foo", "prop1" to "bar"),
                startNode = MockNode(id = 1, labels = mutableListOf(Label.label("Foo"), Label.label("Bar")),
                        properties = mutableMapOf("prop" to "foo", "prop1" to "bar")),
                endNode = MockNode(id = 2, labels = mutableListOf(Label.label("FooEnd"), Label.label("BarEnd")),
                        properties = mutableMapOf("prop" to "fooEnd", "prop1" to "barEnd")))

        // When
        val relationshipPayload = streamsProcedures.buildPayload("neo4j", relationship)

        // Then
        assertTrue { relationshipPayload is Map<*, *> }
        val relationshipMap = relationshipPayload as Map<String, Any>
        assertEquals(relationship.id.toString(), relationshipMap["id"])
        assertEquals(relationship.type, relationshipMap["label"])
        assertEquals(relationship.properties, relationshipMap["properties"])
        val startNodeRelMap = relationshipMap["start"] as RelationshipNodeChange
        assertEquals(relationship.startNode.id.toString(), startNodeRelMap.id)
        assertEquals(relationship.startNode.labelNames(), startNodeRelMap.labels)
        val endNodeRelMap = relationshipMap["end"] as RelationshipNodeChange
        assertEquals(relationship.endNode.id.toString(), endNodeRelMap.id)
        assertEquals(relationship.endNode.labelNames(), endNodeRelMap.labels)
    }

    @Test
    fun shouldCreateRelationshipWithIncludedProperties() {
        // Given
        val relRouting = RelationshipRoutingConfiguration(all = false, name = "KNOWS", include = listOf("prop1"))
        val dependencyResolver = MockDependencyResolver(relRouting = listOf(relRouting))
        streamsProcedures.db = MockGraphDatabaseAPI(dependencyResolver)
        val relationship = MockRelationship(id = 10, type = "KNOWS", properties = mutableMapOf("prop" to "foo", "prop1" to "bar"),
                startNode = MockNode(id = 1, labels = mutableListOf(Label.label("Foo"), Label.label("Bar")),
                        properties = mutableMapOf("prop" to "foo", "prop1" to "bar")),
                endNode = MockNode(id = 2, labels = mutableListOf(Label.label("FooEnd"), Label.label("BarEnd")),
                        properties = mutableMapOf("prop" to "fooEnd", "prop1" to "barEnd")))

        // When
        val relationshipPayload = streamsProcedures.buildPayload("neo4j", relationship)

        // Then
        assertTrue { relationshipPayload is Map<*, *> }
        val relationshipMap = relationshipPayload as Map<String, Any>
        assertEquals(relationship.id.toString(), relationshipMap["id"])
        assertEquals(relationship.type, relationshipMap["label"])
        assertEquals(relationship.properties.filter { relRouting.include.contains(it.key) }, relationshipMap["properties"])
        val startNodeRelMap = relationshipMap["start"] as RelationshipNodeChange
        assertEquals(relationship.startNode.id.toString(), startNodeRelMap.id)
        assertEquals(relationship.startNode.labelNames(), startNodeRelMap.labels)
        val endNodeRelMap = relationshipMap["end"] as RelationshipNodeChange
        assertEquals(relationship.endNode.id.toString(), endNodeRelMap.id)
        assertEquals(relationship.endNode.labelNames(), endNodeRelMap.labels)
    }

    @Test
    fun shouldCreateRelationshipWithoutExcludedProperties() {
        // Given
        val relRouting = RelationshipRoutingConfiguration(all = false, name = "KNOWS", exclude = listOf("prop1"))
        val dependencyResolver = MockDependencyResolver(relRouting = listOf(relRouting))
        streamsProcedures.db = MockGraphDatabaseAPI(dependencyResolver)
        val relationship = MockRelationship(id = 10, type = "KNOWS", properties = mutableMapOf("prop" to "foo", "prop1" to "bar"),
                startNode = MockNode(id = 1, labels = mutableListOf(Label.label("Foo"), Label.label("Bar")),
                        properties = mutableMapOf("prop" to "foo", "prop1" to "bar")),
                endNode = MockNode(id = 2, labels = mutableListOf(Label.label("FooEnd"), Label.label("BarEnd")),
                        properties = mutableMapOf("prop" to "fooEnd", "prop1" to "barEnd")))

        // When
        val relationshipPayload = streamsProcedures.buildPayload("neo4j", relationship)

        // Then
        assertTrue { relationshipPayload is Map<*, *> }
        val relationshipMap = relationshipPayload as Map<String, Any>
        assertEquals(relationship.id.toString(), relationshipMap["id"])
        assertEquals(relationship.type, relationshipMap["label"])
        assertEquals(relationship.properties.filter { !relRouting.exclude.contains(it.key) }, relationshipMap["properties"])
        val startNodeRelMap = relationshipMap["start"] as RelationshipNodeChange
        assertEquals(relationship.startNode.id.toString(), startNodeRelMap.id)
        assertEquals(relationship.startNode.labelNames(), startNodeRelMap.labels)
        val endNodeRelMap = relationshipMap["end"] as RelationshipNodeChange
        assertEquals(relationship.endNode.id.toString(), endNodeRelMap.id)
        assertEquals(relationship.endNode.labelNames(), endNodeRelMap.labels)
    }

    @Test
    fun shouldReturnSimpleMap() {
        // Given
        streamsProcedures.db = MockGraphDatabaseAPI()
        val expectedMap = mapOf("foo" to "bar", "bar" to 10, "prop" to listOf(1, "two", null, mapOf("foo" to "bar")))

        // When
        val resultMap = streamsProcedures.buildPayload("neo4j", expectedMap)

        // Then
        assertEquals(expectedMap, resultMap)
    }

    @Test
    fun shouldReturnSimpleList() {
        // Given
        streamsProcedures.db = MockGraphDatabaseAPI()
        val expectedList = listOf("3", 2, 1, mapOf("foo" to "bar", "bar" to 10, "prop" to listOf(1, "two", null, mapOf("foo" to "bar"))))

        // When
        val resultList = streamsProcedures.buildPayload("neo4j", expectedList)

        // Then
        assertEquals(expectedList, resultList)
    }

    @Test
    fun shouldReturnMapWithComplexTypes() {
        // Given
        streamsProcedures.db = MockGraphDatabaseAPI()
        val node = MockNode(id = 1, labels = mutableListOf(Label.label("Foo"), Label.label("Bar")),
                properties = mutableMapOf("prop" to "foo", "prop1" to "bar"))
        val relationship = MockRelationship(id = 10, type = "KNOWS", properties = mutableMapOf("prop" to "foo", "prop1" to "bar"),
                startNode = MockNode(id = 1, labels = mutableListOf(Label.label("Foo"), Label.label("Bar")),
                        properties = mutableMapOf("prop" to "foo", "prop1" to "bar")),
                endNode = MockNode(id = 2, labels = mutableListOf(Label.label("FooEnd"), Label.label("BarEnd")),
                        properties = mutableMapOf("prop" to "fooEnd", "prop1" to "barEnd")))
        val mapComplex = mapOf("node" to node, "relationship" to relationship, "prop" to listOf(1, "two", null, mapOf("foo" to "bar")))

        // When
        val resultMapComplex = streamsProcedures.buildPayload("neo4j", mapComplex)

        // Then
        val expectedMapComplex = mapComplex.mapValues {
            if (it.key == "node" || it.key == "relationship") {
                streamsProcedures.buildPayload("neo4j", it.value)
            } else {
                it.value
            }
        }
        assertEquals(expectedMapComplex, resultMapComplex)
    }

    @Test
    fun shouldReturnMapWithComplexTypesFiltered() {
        // Given
        val nodeRouting = NodeRoutingConfiguration(all = false, labels = listOf("Foo"), include = listOf("prop1"))
        val relRouting = RelationshipRoutingConfiguration(all = false, name = "KNOWS", include = listOf("prop1"))
        val dependencyResolver = MockDependencyResolver(nodeRouting = listOf(nodeRouting), relRouting = listOf(relRouting))
        streamsProcedures.db = MockGraphDatabaseAPI(dependencyResolver)
        val node = MockNode(id = 1, labels = mutableListOf(Label.label("Foo"), Label.label("Bar")),
                properties = mutableMapOf("prop" to "foo", "prop1" to "bar"))
        val relationship = MockRelationship(id = 10, type = "KNOWS", properties = mutableMapOf("prop" to "foo", "prop1" to "bar"),
                startNode = MockNode(id = 1, labels = mutableListOf(Label.label("Foo"), Label.label("Bar")),
                        properties = mutableMapOf("prop" to "foo", "prop1" to "bar")),
                endNode = MockNode(id = 2, labels = mutableListOf(Label.label("FooEnd"), Label.label("BarEnd")),
                        properties = mutableMapOf("prop" to "fooEnd", "prop1" to "barEnd")))
        val mapComplex = mapOf("node" to node, "relationship" to relationship, "prop" to listOf(1, "two", null, mapOf("foo" to "bar")))

        // When
        val resultMapComplex = streamsProcedures.buildPayload("neo4j", mapComplex)

        // Then
        val expectedMapComplex = mapComplex.mapValues {
            if (it.key == "node" || it.key == "relationship") {
                streamsProcedures.buildPayload("neo4j", it.value)
            } else {
                it.value
            }
        }
        assertEquals(expectedMapComplex, resultMapComplex)
    }

    @Test
    fun shouldReturnPath() {
        // Given
        streamsProcedures.db = MockGraphDatabaseAPI()
        val relationship = MockRelationship(id = 10, type = "KNOWS", properties = mutableMapOf("prop" to "foo", "prop1" to "bar"),
                startNode = MockNode(id = 1, labels = mutableListOf(Label.label("Foo"), Label.label("Bar")),
                        properties = mutableMapOf("prop" to "foo", "prop1" to "bar")),
                endNode = MockNode(id = 2, labels = mutableListOf(Label.label("FooEnd"), Label.label("BarEnd")),
                        properties = mutableMapOf("prop" to "fooEnd", "prop1" to "barEnd")))
        val path = MockPath(relationship = relationship, startNode = relationship.startNode, endNode = relationship.endNode)

        // When
        val resultPath = streamsProcedures.buildPayload("neo4j", path)

        // Then
        val nodes = path.nodes().map { streamsProcedures.buildPayload("neo4j", it) }
        val rels = path.relationships().map { streamsProcedures.buildPayload("neo4j", it) }
        val expectedPath = mapOf("length" to 1, "nodes" to nodes, "rels" to rels)
        assertEquals(expectedPath, resultPath)
    }

    @Test
    fun shouldReturnPathWithFilteredProperties() {
        // Given
        val nodeRouting = NodeRoutingConfiguration(all = false, labels = listOf("Foo"), include = listOf("prop1"))
        val relRouting = RelationshipRoutingConfiguration(all = false, name = "KNOWS", include = listOf("prop1"))
        val dependencyResolver = MockDependencyResolver(nodeRouting = listOf(nodeRouting), relRouting = listOf(relRouting))
        streamsProcedures.db = MockGraphDatabaseAPI(dependencyResolver)
        val relationship = MockRelationship(id = 10, type = "KNOWS", properties = mutableMapOf("prop" to "foo", "prop1" to "bar"),
                startNode = MockNode(id = 1, labels = mutableListOf(Label.label("Foo"), Label.label("Bar")),
                        properties = mutableMapOf("prop" to "foo", "prop1" to "bar")),
                endNode = MockNode(id = 2, labels = mutableListOf(Label.label("FooEnd"), Label.label("BarEnd")),
                        properties = mutableMapOf("prop" to "fooEnd", "prop1" to "barEnd")))
        val path = MockPath(relationship = relationship, startNode = relationship.startNode, endNode = relationship.endNode)

        // When
        val resultPath = streamsProcedures.buildPayload("neo4j", path)

        // Then
        val nodes = path.nodes().map { streamsProcedures.buildPayload("neo4j", it) }
        val rels = path.relationships().map { streamsProcedures.buildPayload("neo4j", it) }
        val expectedPath = mapOf("length" to 1, "nodes" to nodes, "rels" to rels)
        assertEquals(expectedPath, resultPath)
    }

}

