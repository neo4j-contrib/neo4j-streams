package streams.events

import org.junit.Test
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import streams.NodeRoutingConfiguration
import streams.RelationshipRoutingConfiguration
import streams.mocks.MockNode
import streams.mocks.MockPath
import streams.mocks.MockRelationship
import streams.toMap
import kotlin.test.assertEquals

class StreamsEventBuilderTest {

    @Test
    fun shouldCreateSimpleTypes() {
        // Given
        val payload = "Test"

        // When
        val result = StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(payload)
                .build()

        // Then
        assertEquals(payload, result.payload)
    }

    @Test
    fun shouldCreateNode() {
        // Given
        val payload = MockNode(id = 1, labels = mutableListOf(Label.label("Foo"), Label.label("Bar")),
                properties = mutableMapOf("prop" to "foo", "prop1" to "bar"))

        // When
        val result = StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(payload)
                .build()

        // Then
        assertEquals(payload.toMap(), result.payload)
    }

    @Test
    fun shouldCreateNodeWithIncludedProperties() {
        // Given
        val nodeRouting = NodeRoutingConfiguration(all = false, labels = listOf("Foo"), include = listOf("prop1"))
        val payload = MockNode(id = 1, labels = mutableListOf(Label.label("Foo"), Label.label("Bar")),
                properties = mutableMapOf("prop" to "foo", "prop1" to "bar"))

        // When
        val result = StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(payload)
                .withNodeRoutingConfiguration(nodeRouting)
                .build()

        // Then
        val payloadAsMap = payload.toMap().toMutableMap()
        payloadAsMap["properties"] = payload.properties.filter { nodeRouting.include.contains(it.key) }
        val expected = payloadAsMap.toMap()
        assertEquals(expected, result.payload)
    }

    @Test
    fun shouldCreateNodeWithoutExcludedProperties() {
        // Given
        val nodeRouting = NodeRoutingConfiguration(all = false, labels = listOf("Foo"), exclude = listOf("prop1"))
        val payload = MockNode(id = 1, labels = mutableListOf(Label.label("Foo"), Label.label("Bar")),
                properties = mutableMapOf("prop" to "foo", "prop1" to "bar"))

        // When
        val result = StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(payload)
                .withNodeRoutingConfiguration(nodeRouting)
                .build()

        // Then
        val payloadAsMap = payload.toMap().toMutableMap()
        payloadAsMap["properties"] = payload.properties.filter { !nodeRouting.exclude.contains(it.key) }
        val expected = payloadAsMap.toMap()
        assertEquals(expected, result.payload)
    }

    @Test
    fun shouldCreateRelationship() {
        // Given
        val payload = MockRelationship(id = 10, type = "KNOWS", properties = mutableMapOf("prop" to "foo", "prop1" to "bar"),
                startNode = MockNode(id = 1, labels = mutableListOf(Label.label("Foo"), Label.label("Bar")),
                        properties = mutableMapOf("prop" to "foo", "prop1" to "bar")),
                endNode = MockNode(id = 2, labels = mutableListOf(Label.label("FooEnd"), Label.label("BarEnd")),
                        properties = mutableMapOf("prop" to "fooEnd", "prop1" to "barEnd")))

        // When
        val result = StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(payload)
                .build()

        // Then
        assertEquals(payload.toMap(), result.payload)
    }

    @Test
    fun shouldCreateRelationshipWithIncludedProperties() {
        // Given
        val relRouting = RelationshipRoutingConfiguration(all = false, name = "KNOWS", include = listOf("prop1"))
        val payload = MockRelationship(id = 10, type = "KNOWS", properties = mutableMapOf("prop" to "foo", "prop1" to "bar"),
                startNode = MockNode(id = 1, labels = mutableListOf(Label.label("Foo"), Label.label("Bar")),
                        properties = mutableMapOf("prop" to "foo", "prop1" to "bar")),
                endNode = MockNode(id = 2, labels = mutableListOf(Label.label("FooEnd"), Label.label("BarEnd")),
                        properties = mutableMapOf("prop" to "fooEnd", "prop1" to "barEnd")))

        // When
        val result = StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(payload)
                .withRelationshipRoutingConfiguration(relRouting)
                .build()

        // Then
        val payloadAsMap = payload.toMap().toMutableMap()
        payloadAsMap["properties"] = payload.properties.filter { relRouting.include.contains(it.key) }
        assertEquals(payloadAsMap.toMap(), result.payload)
    }

    @Test
    fun shouldCreateRelationshipWithoutExcludedProperties() {
        // Given
        val relRouting = RelationshipRoutingConfiguration(all = false, name = "KNOWS", exclude = listOf("prop1"))
        val payload = MockRelationship(id = 10, type = "KNOWS", properties = mutableMapOf("prop" to "foo", "prop1" to "bar"),
                startNode = MockNode(id = 1, labels = mutableListOf(Label.label("Foo"), Label.label("Bar")),
                        properties = mutableMapOf("prop" to "foo", "prop1" to "bar")),
                endNode = MockNode(id = 2, labels = mutableListOf(Label.label("FooEnd"), Label.label("BarEnd")),
                        properties = mutableMapOf("prop" to "fooEnd", "prop1" to "barEnd")))

        // When
        val result = StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(payload)
                .withRelationshipRoutingConfiguration(relRouting)
                .build()

        // Then
        val payloadAsMap = payload.toMap().toMutableMap()
        payloadAsMap["properties"] = payload.properties.filter { !relRouting.exclude.contains(it.key) }
        assertEquals(payloadAsMap.toMap(), result.payload)
    }

    @Test
    fun shouldReturnSimpleMap() {
        // Given
        val payload = mapOf("foo" to "bar", "bar" to 10, "prop" to listOf(1, "two", null, mapOf("foo" to "bar")))

        // When
        val result = StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(payload)
                .build()

        // Then
        assertEquals(payload, result.payload)
    }

    @Test
    fun shouldReturnSimpleList() {
        // Given
        val payload = listOf("3", 2, 1, mapOf("foo" to "bar", "bar" to 10, "prop" to listOf(1, "two", null, mapOf("foo" to "bar"))))

        // When
        val result = StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(payload)
                .build()

        // Then
        assertEquals(payload, result.payload)
    }

    @Test
    fun shouldReturnMapWithComplexTypes() {
        // Given
        val node = MockNode(id = 1, labels = mutableListOf(Label.label("Foo"), Label.label("Bar")),
                properties = mutableMapOf("prop" to "foo", "prop1" to "bar"))
        val relationship = MockRelationship(id = 10, type = "KNOWS", properties = mutableMapOf("prop" to "foo", "prop1" to "bar"),
                startNode = MockNode(id = 1, labels = mutableListOf(Label.label("Foo"), Label.label("Bar")),
                        properties = mutableMapOf("prop" to "foo", "prop1" to "bar")),
                endNode = MockNode(id = 2, labels = mutableListOf(Label.label("FooEnd"), Label.label("BarEnd")),
                        properties = mutableMapOf("prop" to "fooEnd", "prop1" to "barEnd")))
        val payload = mapOf("node" to node, "relationship" to relationship, "prop" to listOf(1, "two", null, mapOf("foo" to "bar")))

        // When
        val result = StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(payload)
                .build()

        // Then
        val payloadAsMutableMap = payload.toMutableMap()
        payloadAsMutableMap["node"] = (payloadAsMutableMap["node"] as Node).toMap()
        payloadAsMutableMap["relationship"] = (payloadAsMutableMap["relationship"] as Relationship).toMap()
        assertEquals(payloadAsMutableMap.toMap(), result.payload)
    }

    @Test
    fun shouldReturnMapWithComplexTypesFiltered() {
        // Given
        val nodeRouting = NodeRoutingConfiguration(all = false, labels = listOf("Foo"), include = listOf("prop1"))
        val relRouting = RelationshipRoutingConfiguration(all = false, name = "KNOWS", include = listOf("prop1"))
        val node = MockNode(id = 1, labels = mutableListOf(Label.label("Foo"), Label.label("Bar")),
                properties = mutableMapOf("prop" to "foo", "prop1" to "bar"))
        val relationship = MockRelationship(id = 10, type = "KNOWS", properties = mutableMapOf("prop" to "foo", "prop1" to "bar"),
                startNode = MockNode(id = 1, labels = mutableListOf(Label.label("Foo"), Label.label("Bar")),
                        properties = mutableMapOf("prop" to "foo", "prop1" to "bar")),
                endNode = MockNode(id = 2, labels = mutableListOf(Label.label("FooEnd"), Label.label("BarEnd")),
                        properties = mutableMapOf("prop" to "fooEnd", "prop1" to "barEnd")))
        val payload = mapOf("node" to node, "relationship" to relationship, "prop" to listOf(1, "two", null, mapOf("foo" to "bar")))

        // When
        val resultNode = StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(node)
                .withNodeRoutingConfiguration(nodeRouting)
                .build()
        val resultRelationship = StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(relationship)
                .withRelationshipRoutingConfiguration(relRouting)
                .build()
        val result = StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(payload)
                .withRelationshipRoutingConfiguration(relRouting)
                .withNodeRoutingConfiguration(nodeRouting)
                .build()

        // Then
        val payloadAsMutableMap = payload.toMutableMap()
        payloadAsMutableMap["node"] = resultNode.payload
        payloadAsMutableMap["relationship"] = resultRelationship.payload
        assertEquals(payloadAsMutableMap.toMap(), result.payload)
    }

    @Test
    fun shouldReturnPath() {
        // Given
        val relationship = MockRelationship(id = 10, type = "KNOWS", properties = mutableMapOf("prop" to "foo", "prop1" to "bar"),
                startNode = MockNode(id = 1, labels = mutableListOf(Label.label("Foo"), Label.label("Bar")),
                        properties = mutableMapOf("prop" to "foo", "prop1" to "bar")),
                endNode = MockNode(id = 2, labels = mutableListOf(Label.label("FooEnd"), Label.label("BarEnd")),
                        properties = mutableMapOf("prop" to "fooEnd", "prop1" to "barEnd")))
        val payload = MockPath(relationship = relationship, startNode = relationship.startNode, endNode = relationship.endNode)

        // When
        val result = StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(payload)
                .build()

        // Then
        val nodes = payload.nodes().map { StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(it)
                .build()
                .payload
        }
        val rels = payload.relationships().map { StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(it)
                .build()
                .payload
        }
        val expectedPath = mapOf("length" to 1, "nodes" to nodes, "rels" to rels)
        assertEquals(expectedPath, result.payload)
    }

    @Test
    fun shouldReturnPathWithFilteredProperties() {
        // Given
        val nodeRouting = NodeRoutingConfiguration(all = false, labels = listOf("Foo"), include = listOf("prop1"))
        val relRouting = RelationshipRoutingConfiguration(all = false, name = "KNOWS", include = listOf("prop1"))
        val relationship = MockRelationship(id = 10, type = "KNOWS", properties = mutableMapOf("prop" to "foo", "prop1" to "bar"),
                startNode = MockNode(id = 1, labels = mutableListOf(Label.label("Foo"), Label.label("Bar")),
                        properties = mutableMapOf("prop" to "foo", "prop1" to "bar")),
                endNode = MockNode(id = 2, labels = mutableListOf(Label.label("FooEnd"), Label.label("BarEnd")),
                        properties = mutableMapOf("prop" to "fooEnd", "prop1" to "barEnd")))
        val payload = MockPath(relationship = relationship, startNode = relationship.startNode, endNode = relationship.endNode)

        // When
        val result = StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(payload)
                .withRelationshipRoutingConfiguration(relRouting)
                .withNodeRoutingConfiguration(nodeRouting)
                .build()

        // Then
        val nodes = payload.nodes().map { StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(it)
                .withNodeRoutingConfiguration(nodeRouting)
                .build()
                .payload
        }
        val rels = payload.relationships().map { StreamsEventBuilder()
                .withTopic("neo4j")
                .withPayload(it)
                .withRelationshipRoutingConfiguration(relRouting)
                .build()
                .payload
        }
        val expectedPath = mapOf("length" to 1, "nodes" to nodes, "rels" to rels)
        assertEquals(expectedPath, result.payload)
    }

}

