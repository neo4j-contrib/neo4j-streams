package streams.service.sink.strategy

import org.junit.Test
import streams.extensions.quote
import streams.service.StreamsSinkEntity
import streams.utils.StreamsUtils
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class CUDIngestionStrategyTest {

    private fun findEventByQuery(query: String, evts: List<QueryEvents>) = evts.find { it.query == query }!!

    private fun assertNodeEventsContainsKey(qe: QueryEvents, vararg keys: String) = assertTrue {
        qe.events.all {
            val ids = it[CUDIngestionStrategy.ID_KEY] as Map<String, Any>
            ids.keys.containsAll(keys.toList())
        }
    }

    private fun assertRelationshipEventsContainsKey(qe: QueryEvents, fromKey: String, toKey: String) = assertTrue {
            qe.events.all {
                val from = it["from"] as Map<String, Any>
                val idsFrom = from[CUDIngestionStrategy.ID_KEY] as Map<String, Any>
                val to = it["to"] as Map<String, Any>
                val idsTo = to[CUDIngestionStrategy.ID_KEY] as Map<String, Any>
                idsFrom.containsKey(fromKey) && idsTo.containsKey(toKey)
            }
        }

    @Test
    fun `should create, merge and update nodes`() {
        // given
        val mergeMarkers = listOf(2, 5, 7)
        val updateMarkers = listOf(3, 6)
        val key = "key"
        val list = (1..10).map {
            val labels = if (it % 2 == 0) listOf("Foo", "Bar") else listOf("Foo", "Bar", "Label")
            val properties = mapOf("foo" to "foo-value-$it", "id" to it)
            val (op, ids) = when (it) {
                in mergeMarkers -> CUDOperations.merge to mapOf(key to it)
                in updateMarkers -> CUDOperations.update to mapOf(key to it)
                else -> CUDOperations.create to emptyMap()
            }
            val cudNode = CUDNode(op = op,
                    labels = labels,
                    ids = ids,
                    properties = properties)
            StreamsSinkEntity(null, cudNode)
        }

        // when
        val cudQueryStrategy = CUDIngestionStrategy()
        val nodeEvents = cudQueryStrategy.mergeNodeEvents(list)
        val nodeDeleteEvents = cudQueryStrategy.deleteNodeEvents(list)

        val relationshipEvents = cudQueryStrategy.mergeRelationshipEvents(list)
        val relationshipDeleteEvents = cudQueryStrategy.deleteRelationshipEvents(list)

        // then
        assertEquals(emptyList(), nodeDeleteEvents)
        assertEquals(emptyList(), relationshipEvents)
        assertEquals(emptyList(), relationshipDeleteEvents)

        assertEquals(6, nodeEvents.size)
        assertEquals(10, nodeEvents.map { it.events.size }.sum())
        val createNodeFooBar = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |CREATE (n:Foo:Bar)
                |SET n = event.properties
            """.trimMargin(), nodeEvents)
        assertEquals(3, createNodeFooBar.events.size)
        val createNodeFooBarLabel = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |CREATE (n:Foo:Bar:Label)
                |SET n = event.properties
            """.trimMargin(), nodeEvents)
        assertEquals(2, createNodeFooBarLabel.events.size)
        val mergeNodeFooBar = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MERGE (n:Foo:Bar {${key.quote()}: event.${CUDIngestionStrategy.ID_KEY}.${key.quote()}})
                |SET n += event.properties
            """.trimMargin(), nodeEvents)
        assertEquals(1, mergeNodeFooBar.events.size)
        assertNodeEventsContainsKey(mergeNodeFooBar, key)
        val mergeNodeFooBarLabel = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MERGE (n:Foo:Bar:Label {${key.quote()}: event.${CUDIngestionStrategy.ID_KEY}.${key.quote()}})
                |SET n += event.properties
            """.trimMargin(), nodeEvents)
        assertEquals(2, mergeNodeFooBarLabel.events.size)
        assertNodeEventsContainsKey(mergeNodeFooBarLabel, key)
        val updateNodeFooBar = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MATCH (n:Foo:Bar {${key.quote()}: event.${CUDIngestionStrategy.ID_KEY}.${key.quote()}})
                |SET n += event.properties
            """.trimMargin(), nodeEvents)
        assertEquals(1, updateNodeFooBar.events.size)
        assertNodeEventsContainsKey(updateNodeFooBar, key)
        val updateNodeFooBarLabel = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MATCH (n:Foo:Bar:Label {${key.quote()}: event.${CUDIngestionStrategy.ID_KEY}.${key.quote()}})
                |SET n += event.properties
            """.trimMargin(), nodeEvents)
        assertEquals(1, updateNodeFooBarLabel.events.size)
        assertNodeEventsContainsKey(updateNodeFooBarLabel, key)
    }

    @Test
    fun `should create, merge, update and delete nodes with garbage data`() {
        // given
        val mergeMarkers = listOf(2, 5, 7)
        val updateMarkers = listOf(3, 6)
        val deleteMarkers = listOf(10)
        val key = "not..... SO SIMPLE!"
        val list = (1..10).map {
            val labels = if (it % 2 == 0) listOf("WellBehaved", "C̸r̵a̵z̵y̵ ̶.̵ ̶ ̴ ̸ ̶ ̶ ̵ ̴L̴a̵b̸e̶l") else listOf("WellBehaved", "C̸r̵a̵z̵y̵ ̶.̵ ̶ ̴ ̸ ̶ ̶ ̵ ̴L̴a̵b̸e̶l", "Label")
            val properties = if (it in deleteMarkers) emptyMap() else mapOf("foo" to "foo-value-$it", "id" to it)
            val (op, ids) = when (it) {
                in mergeMarkers -> CUDOperations.merge to mapOf(key to it)
                in updateMarkers -> CUDOperations.update to mapOf(key to it)
                in deleteMarkers -> CUDOperations.delete to mapOf(key to it)
                else -> CUDOperations.create to emptyMap()
            }
            val cudNode = CUDNode(op = op,
                    labels = labels,
                    ids = ids,
                    properties = properties)
            StreamsSinkEntity(null, cudNode)
        }

        // when
        val cudQueryStrategy = CUDIngestionStrategy()
        val nodeEvents = cudQueryStrategy.mergeNodeEvents(list)
        val nodeDeleteEvents = cudQueryStrategy.deleteNodeEvents(list)

        val relationshipEvents = cudQueryStrategy.mergeRelationshipEvents(list)
        val relationshipDeleteEvents = cudQueryStrategy.deleteRelationshipEvents(list)

        // then
        assertEquals(emptyList(), relationshipEvents)
        assertEquals(emptyList(), relationshipDeleteEvents)

        assertEquals(6, nodeEvents.size)
        assertEquals(9, nodeEvents.map { it.events.size }.sum())
        val createNodeFooBar = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |CREATE (n:WellBehaved:`C̸r̵a̵z̵y̵ ̶.̵ ̶ ̴ ̸ ̶ ̶ ̵ ̴L̴a̵b̸e̶l`)
                |SET n = event.properties
            """.trimMargin(), nodeEvents)
        assertEquals(2, createNodeFooBar.events.size)
        val createNodeFooBarLabel = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |CREATE (n:WellBehaved:`C̸r̵a̵z̵y̵ ̶.̵ ̶ ̴ ̸ ̶ ̶ ̵ ̴L̴a̵b̸e̶l`:Label)
                |SET n = event.properties
            """.trimMargin(), nodeEvents)
        assertEquals(2, createNodeFooBarLabel.events.size)
        val mergeNodeFooBar = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MERGE (n:WellBehaved:`C̸r̵a̵z̵y̵ ̶.̵ ̶ ̴ ̸ ̶ ̶ ̵ ̴L̴a̵b̸e̶l` {`$key`: event.${CUDIngestionStrategy.ID_KEY}.`$key`})
                |SET n += event.properties
            """.trimMargin(), nodeEvents)
        assertEquals(1, mergeNodeFooBar.events.size)
        assertNodeEventsContainsKey(mergeNodeFooBar, key)
        val mergeNodeFooBarLabel = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MERGE (n:WellBehaved:`C̸r̵a̵z̵y̵ ̶.̵ ̶ ̴ ̸ ̶ ̶ ̵ ̴L̴a̵b̸e̶l`:Label {`$key`: event.${CUDIngestionStrategy.ID_KEY}.`$key`})
                |SET n += event.properties
            """.trimMargin(), nodeEvents)
        assertEquals(2, mergeNodeFooBarLabel.events.size)
        assertNodeEventsContainsKey(mergeNodeFooBarLabel, key)
        val updateNodeFooBar = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MATCH (n:WellBehaved:`C̸r̵a̵z̵y̵ ̶.̵ ̶ ̴ ̸ ̶ ̶ ̵ ̴L̴a̵b̸e̶l` {`$key`: event.${CUDIngestionStrategy.ID_KEY}.`$key`})
                |SET n += event.properties
            """.trimMargin(), nodeEvents)
        assertEquals(1, updateNodeFooBar.events.size)
        assertNodeEventsContainsKey(updateNodeFooBar, key)
        val updateNodeFooBarLabel = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MATCH (n:WellBehaved:`C̸r̵a̵z̵y̵ ̶.̵ ̶ ̴ ̸ ̶ ̶ ̵ ̴L̴a̵b̸e̶l`:Label {`$key`: event.${CUDIngestionStrategy.ID_KEY}.`$key`})
                |SET n += event.properties
            """.trimMargin(), nodeEvents)
        assertEquals(1, updateNodeFooBarLabel.events.size)
        assertNodeEventsContainsKey(updateNodeFooBarLabel, key)

        assertEquals(1, nodeDeleteEvents.size)
        val nodeDeleteEvent = nodeDeleteEvents.first()
        assertEquals("""
                |${StreamsUtils.UNWIND}
                |MATCH (n:WellBehaved:`C̸r̵a̵z̵y̵ ̶.̵ ̶ ̴ ̸ ̶ ̶ ̵ ̴L̴a̵b̸e̶l` {`$key`: event.${CUDIngestionStrategy.ID_KEY}.`$key`})
                |DETACH DELETE n
            """.trimMargin(), nodeDeleteEvent.query)
    }

    @Test
    fun `should delete nodes`() {
        // given
        val detachMarkers = listOf(1, 3, 8, 10)
        val key = "key"
        val list = (1..10).map {
            val labels = if (it % 2 == 0) listOf("Foo", "Bar") else listOf("Foo", "Bar", "Label")
            val detach = it in detachMarkers
            val properties = emptyMap<String, Any>()
            val cudNode = CUDNode(op = CUDOperations.delete,
                    labels = labels,
                    ids = mapOf(key to it),
                    properties = properties,
                    detach = detach)
            StreamsSinkEntity(null, cudNode)
        }

        // when
        val cudQueryStrategy = CUDIngestionStrategy()
        val nodeEvents = cudQueryStrategy.mergeNodeEvents(list)
        val nodeDeleteEvents = cudQueryStrategy.deleteNodeEvents(list)

        val relationshipEvents = cudQueryStrategy.mergeRelationshipEvents(list)
        val relationshipDeleteEvents = cudQueryStrategy.deleteRelationshipEvents(list)

        // then
        assertEquals(emptyList(), nodeEvents)
        assertEquals(emptyList(), relationshipEvents)
        assertEquals(emptyList(), relationshipDeleteEvents)

        assertEquals(4, nodeDeleteEvents.size)
        assertEquals(10, nodeDeleteEvents.map { it.events.size }.sum())
        val deleteNodeFooBar = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MATCH (n:Foo:Bar {${key.quote()}: event.${CUDIngestionStrategy.ID_KEY}.${key.quote()}})
                |DELETE n
            """.trimMargin(), nodeDeleteEvents)
        assertEquals(3, deleteNodeFooBar.events.size)
        assertNodeEventsContainsKey(deleteNodeFooBar, key)
        val deleteNodeFooBarDetach = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MATCH (n:Foo:Bar {${key.quote()}: event.${CUDIngestionStrategy.ID_KEY}.${key.quote()}})
                |DETACH DELETE n
            """.trimMargin(), nodeDeleteEvents)
        assertEquals(2, deleteNodeFooBarDetach.events.size)
        assertNodeEventsContainsKey(deleteNodeFooBarDetach, key)
        val deleteNodeFooBarLabel = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MATCH (n:Foo:Bar:Label {${key.quote()}: event.${CUDIngestionStrategy.ID_KEY}.${key.quote()}})
                |DELETE n
            """.trimMargin(), nodeDeleteEvents)
        assertEquals(3, deleteNodeFooBarLabel.events.size)
        assertNodeEventsContainsKey(deleteNodeFooBarLabel, key)
        val deleteNodeFooBarLabelDetach = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MATCH (n:Foo:Bar:Label {${key.quote()}: event.${CUDIngestionStrategy.ID_KEY}.${key.quote()}})
                |DETACH DELETE n
            """.trimMargin(), nodeDeleteEvents)
        assertEquals(2, deleteNodeFooBarLabelDetach.events.size)
        assertNodeEventsContainsKey(deleteNodeFooBarLabelDetach, key)
    }

    @Test
    fun `should delete nodes with internal id reference`() {
        // given
        val detachMarkers = listOf(1, 3, 8, 10)
        val list = (1..10).map {
            val labels = if (it % 2 == 0) listOf("Foo", "Bar") else listOf("Foo", "Bar", "Label")
            val detach = it in detachMarkers
            val properties = emptyMap<String, Any>()
            val cudNode = CUDNode(op = CUDOperations.delete,
                    labels = labels,
                    ids = mapOf("_id" to it),
                    properties = properties,
                    detach = detach)
            StreamsSinkEntity(null, cudNode)
        }

        // when
        val cudQueryStrategy = CUDIngestionStrategy()
        val nodeEvents = cudQueryStrategy.mergeNodeEvents(list)
        val nodeDeleteEvents = cudQueryStrategy.deleteNodeEvents(list)

        val relationshipEvents = cudQueryStrategy.mergeRelationshipEvents(list)
        val relationshipDeleteEvents = cudQueryStrategy.deleteRelationshipEvents(list)

        // then
        assertEquals(emptyList(), nodeEvents)
        assertEquals(emptyList(), relationshipEvents)
        assertEquals(emptyList(), relationshipDeleteEvents)

        assertEquals(4, nodeDeleteEvents.size)
        assertEquals(10, nodeDeleteEvents.map { it.events.size }.sum())
        val deleteNodeFooBar = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MATCH (n) WHERE id(n) = event.${CUDIngestionStrategy.ID_KEY}._id
                |DELETE n
            """.trimMargin(), nodeDeleteEvents)
        assertEquals(3, deleteNodeFooBar.events.size)
        val key = "_id"
        assertNodeEventsContainsKey(deleteNodeFooBar, key)
        val deleteNodeFooBarDetach = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MATCH (n) WHERE id(n) = event.${CUDIngestionStrategy.ID_KEY}._id
                |DETACH DELETE n
            """.trimMargin(), nodeDeleteEvents)
        assertEquals(2, deleteNodeFooBarDetach.events.size)
        assertNodeEventsContainsKey(deleteNodeFooBarDetach, key)
        val deleteNodeFooBarLabel = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MATCH (n) WHERE id(n) = event.${CUDIngestionStrategy.ID_KEY}._id
                |DELETE n
            """.trimMargin(), nodeDeleteEvents)
        assertEquals(3, deleteNodeFooBarLabel.events.size)
        assertNodeEventsContainsKey(deleteNodeFooBarLabel, key)
        val deleteNodeFooBarLabelDetach = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MATCH (n) WHERE id(n) = event.${CUDIngestionStrategy.ID_KEY}._id
                |DETACH DELETE n
            """.trimMargin(), nodeDeleteEvents)
        assertEquals(2, deleteNodeFooBarLabelDetach.events.size)
        assertNodeEventsContainsKey(deleteNodeFooBarLabelDetach, key)
    }

    @Test
    fun `should create, merge and update nodes with internal id reference`() {
        // given
        val mergeMarkers = listOf(2, 5, 7)
        val updateMarkers = listOf(3, 6)
        val list = (1..10).map {
            val labels = if (it % 2 == 0) listOf("Foo", "Bar") else listOf("Foo", "Bar", "Label")
            val properties = mapOf("foo" to "foo-value-$it", "id" to it)
            val (op, ids) = when (it) {
                in mergeMarkers -> CUDOperations.merge to mapOf("_id" to it)
                in updateMarkers -> CUDOperations.update to mapOf("_id" to it)
                else -> CUDOperations.create to emptyMap()
            }
            val cudNode = CUDNode(op = op,
                    labels = labels,
                    ids = ids,
                    properties = properties)
            StreamsSinkEntity(null, cudNode)
        }

        // when
        val cudQueryStrategy = CUDIngestionStrategy()
        val nodeEvents = cudQueryStrategy.mergeNodeEvents(list)
        val nodeDeleteEvents = cudQueryStrategy.deleteNodeEvents(list)

        val relationshipEvents = cudQueryStrategy.mergeRelationshipEvents(list)
        val relationshipDeleteEvents = cudQueryStrategy.deleteRelationshipEvents(list)

        // then
        assertEquals(emptyList(), nodeDeleteEvents)
        assertEquals(emptyList(), relationshipEvents)
        assertEquals(emptyList(), relationshipDeleteEvents)

        assertEquals(3, nodeEvents.size)
        assertEquals(10, nodeEvents.map { it.events.size }.sum())
        val createNodeFooBar = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |CREATE (n:Foo:Bar)
                |SET n = event.properties
            """.trimMargin(), nodeEvents)
        assertEquals(3, createNodeFooBar.events.size)
        val createNodeFooBarLabel = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |CREATE (n:Foo:Bar:Label)
                |SET n = event.properties
            """.trimMargin(),nodeEvents)
        assertEquals(2, createNodeFooBarLabel.events.size)
        val mergeNodeFooBar = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MATCH (n) WHERE id(n) = event.ids._id
                |SET n += event.properties
            """.trimMargin(), nodeEvents)
        assertEquals(5, mergeNodeFooBar.events.size)
        assertNodeEventsContainsKey(mergeNodeFooBar, "_id")
    }

    @Test
    fun `should create, merge and update relationships`() {
        // given
        val mergeMarkers = listOf(2, 5, 7)
        val updateMarkers = listOf(3, 6)
        val key = "key"
        val list = (1..10).map {
            val labels = if (it % 2 == 0) listOf("Foo", "Bar") else listOf("Foo", "Bar", "Label")
            val properties = mapOf("foo" to "foo-value-$it", "id" to it)
            val op = when (it) {
                in mergeMarkers -> CUDOperations.merge
                in updateMarkers -> CUDOperations.update
                else -> CUDOperations.create
            }
            val start = CUDNodeRel(ids = mapOf(key to it), labels = labels)
            val end = CUDNodeRel(ids = mapOf(key to it + 1), labels = labels)
            val rel = CUDRelationship(op = op, properties = properties, from = start, to = end, rel_type = "MY_REL")
            StreamsSinkEntity(null, rel)
        }

        // when
        val cudQueryStrategy = CUDIngestionStrategy()
        val nodeEvents = cudQueryStrategy.mergeNodeEvents(list)
        val nodeDeleteEvents = cudQueryStrategy.deleteNodeEvents(list)

        val relationshipEvents = cudQueryStrategy.mergeRelationshipEvents(list)
        val relationshipDeleteEvents = cudQueryStrategy.deleteRelationshipEvents(list)

        // then
        assertEquals(emptyList(), nodeEvents)
        assertEquals(emptyList(), nodeDeleteEvents)
        assertEquals(emptyList(), relationshipDeleteEvents)

        assertEquals(6, relationshipEvents.size)
        assertEquals(10, relationshipEvents.map { it.events.size }.sum())
        val createRelFooBar = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MATCH (from:Foo:Bar {key: event.from.${CUDIngestionStrategy.ID_KEY}.key})
                |MATCH (to:Foo:Bar {key: event.to.${CUDIngestionStrategy.ID_KEY}.key})
                |CREATE (from)-[r:MY_REL]->(to)
                |SET r = event.properties
            """.trimMargin(), relationshipEvents)
        assertEquals(3, createRelFooBar.events.size)
        assertRelationshipEventsContainsKey(createRelFooBar, key, key)
        val mergeRelFooBar = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MATCH (from:Foo:Bar {key: event.from.${CUDIngestionStrategy.ID_KEY}.key})
                |MATCH (to:Foo:Bar {key: event.to.${CUDIngestionStrategy.ID_KEY}.key})
                |MERGE (from)-[r:MY_REL]->(to)
                |SET r += event.properties
            """.trimMargin(), relationshipEvents)
        assertEquals(1, mergeRelFooBar.events.size)
        assertRelationshipEventsContainsKey(mergeRelFooBar, key, key)
        val updateRelFooBar = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MATCH (from:Foo:Bar {key: event.from.${CUDIngestionStrategy.ID_KEY}.key})
                |MATCH (to:Foo:Bar {key: event.to.${CUDIngestionStrategy.ID_KEY}.key})
                |MATCH (from)-[r:MY_REL]->(to)
                |SET r += event.properties
            """.trimMargin(), relationshipEvents)
        assertEquals(1, updateRelFooBar.events.size)
        assertRelationshipEventsContainsKey(updateRelFooBar, key, key)
        val createRelFooBarLabel = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MATCH (from:Foo:Bar:Label {key: event.from.${CUDIngestionStrategy.ID_KEY}.key})
                |MATCH (to:Foo:Bar:Label {key: event.to.${CUDIngestionStrategy.ID_KEY}.key})
                |CREATE (from)-[r:MY_REL]->(to)
                |SET r = event.properties
            """.trimMargin(), relationshipEvents)
        assertEquals(2, createRelFooBarLabel.events.size)
        assertRelationshipEventsContainsKey(createRelFooBarLabel, key, key)
        val mergeRelFooBarLabel = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MATCH (from:Foo:Bar:Label {key: event.from.${CUDIngestionStrategy.ID_KEY}.key})
                |MATCH (to:Foo:Bar:Label {key: event.to.${CUDIngestionStrategy.ID_KEY}.key})
                |MERGE (from)-[r:MY_REL]->(to)
                |SET r += event.properties
            """.trimMargin(), relationshipEvents)
        assertEquals(2, mergeRelFooBarLabel.events.size)
        assertRelationshipEventsContainsKey(mergeRelFooBarLabel, key, key)
        val updateRelFooBarLabel = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MATCH (from:Foo:Bar:Label {key: event.from.${CUDIngestionStrategy.ID_KEY}.key})
                |MATCH (to:Foo:Bar:Label {key: event.to.${CUDIngestionStrategy.ID_KEY}.key})
                |MATCH (from)-[r:MY_REL]->(to)
                |SET r += event.properties
            """.trimMargin(), relationshipEvents)
        assertEquals(1, updateRelFooBarLabel.events.size)
        assertRelationshipEventsContainsKey(updateRelFooBarLabel, key, key)
    }

    @Test
    fun `should delete relationships`() {
        // given
        val key = "key"
        val list = (1..10).map {
            val labels = if (it % 2 == 0) listOf("Foo", "Bar") else listOf("Foo", "Bar", "Label")
            val properties = emptyMap<String, Any>()
            val start = CUDNodeRel(ids = mapOf(key to it), labels = labels)
            val end = CUDNodeRel(ids = mapOf(key to it + 1), labels = labels)
            val rel = CUDRelationship(op = CUDOperations.delete, properties = properties, from = start, to = end, rel_type = "MY_REL")
            StreamsSinkEntity(null, rel)
        }

        // when
        val cudQueryStrategy = CUDIngestionStrategy()
        val nodeEvents = cudQueryStrategy.mergeNodeEvents(list)
        val nodeDeleteEvents = cudQueryStrategy.deleteNodeEvents(list)

        val relationshipEvents = cudQueryStrategy.mergeRelationshipEvents(list)
        val relationshipDeleteEvents = cudQueryStrategy.deleteRelationshipEvents(list)

        // then
        assertEquals(emptyList(), nodeEvents)
        assertEquals(emptyList(), nodeDeleteEvents)
        assertEquals(emptyList(), relationshipEvents)

        assertEquals(2, relationshipDeleteEvents.size)
        assertEquals(10, relationshipDeleteEvents.map { it.events.size }.sum())
        val deleteRelFooBar = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MATCH (from:Foo:Bar {key: event.from.${CUDIngestionStrategy.ID_KEY}.key})
                |MATCH (to:Foo:Bar {key: event.to.${CUDIngestionStrategy.ID_KEY}.key})
                |MATCH (from)-[r:MY_REL]->(to)
                |DELETE r
            """.trimMargin(), relationshipDeleteEvents)
        assertEquals(5, deleteRelFooBar.events.size)
        assertRelationshipEventsContainsKey(deleteRelFooBar, key, key)
        val deleteRelFooBarLabel = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MATCH (from:Foo:Bar:Label {key: event.from.${CUDIngestionStrategy.ID_KEY}.key})
                |MATCH (to:Foo:Bar:Label {key: event.to.${CUDIngestionStrategy.ID_KEY}.key})
                |MATCH (from)-[r:MY_REL]->(to)
                |DELETE r
            """.trimMargin(), relationshipDeleteEvents)
        assertEquals(5, deleteRelFooBarLabel.events.size)
        assertRelationshipEventsContainsKey(deleteRelFooBarLabel, key, key)
    }

    @Test
    fun `should delete relationships with internal id reference`() {
        // given
        val key = "_id"
        val list = (1..10).map {
            val labels = if (it % 2 == 0) listOf("Foo", "Bar") else listOf("Foo", "Bar", "Label")
            val properties = emptyMap<String, Any>()
            val start = CUDNodeRel(ids = mapOf(key to it), labels = labels)
            val relKey = if (it % 2 == 0) key else "key"
            val end = CUDNodeRel(ids = mapOf(relKey to it + 1), labels = labels)
            val rel = CUDRelationship(op = CUDOperations.delete, properties = properties, from = start, to = end, rel_type = "MY_REL")
            StreamsSinkEntity(null, rel)
        }

        // when
        val cudQueryStrategy = CUDIngestionStrategy()
        val nodeEvents = cudQueryStrategy.mergeNodeEvents(list)
        val nodeDeleteEvents = cudQueryStrategy.deleteNodeEvents(list)

        val relationshipEvents = cudQueryStrategy.mergeRelationshipEvents(list)
        val relationshipDeleteEvents = cudQueryStrategy.deleteRelationshipEvents(list)

        // then
        assertEquals(emptyList(), nodeEvents)
        assertEquals(emptyList(), nodeDeleteEvents)
        assertEquals(emptyList(), relationshipEvents)

        assertEquals(2, relationshipDeleteEvents.size)
        assertEquals(10, relationshipDeleteEvents.map { it.events.size }.sum())
        val deleteRel = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MATCH (from) WHERE id(from) = event.from.${CUDIngestionStrategy.ID_KEY}._id
                |MATCH (to) WHERE id(to) = event.to.${CUDIngestionStrategy.ID_KEY}._id
                |MATCH (from)-[r:MY_REL]->(to)
                |DELETE r
            """.trimMargin(), relationshipDeleteEvents)
        assertEquals(5, deleteRel.events.size)
        assertRelationshipEventsContainsKey(deleteRel, key, key)
        val relKey = "key"
        val deleteRelFooBarLabel = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MATCH (from) WHERE id(from) = event.from.${CUDIngestionStrategy.ID_KEY}._id
                |MATCH (to:Foo:Bar:Label {$relKey: event.to.${CUDIngestionStrategy.ID_KEY}.$relKey})
                |MATCH (from)-[r:MY_REL]->(to)
                |DELETE r
            """.trimMargin(), relationshipDeleteEvents)
        assertEquals(5, deleteRelFooBarLabel.events.size)
        assertRelationshipEventsContainsKey(deleteRelFooBarLabel, key, relKey)
    }

    @Test
    fun `should create, merge and update relationships with internal id reference`() {
        // given
        val mergeMarkers = listOf(2, 5, 7)
        val updateMarkers = listOf(3, 6)
        val list = (1..10).map {
            val labels = if (it % 2 == 0) listOf("Foo", "Bar") else listOf("Foo", "Bar", "Label")
            val properties = mapOf("foo" to "foo-value-$it", "id" to it)
            val op = when (it) {
                in mergeMarkers -> CUDOperations.merge
                in updateMarkers -> CUDOperations.update
                else -> CUDOperations.create
            }
            val start = CUDNodeRel(ids = mapOf("_id" to it), labels = labels)
            val end = CUDNodeRel(ids = mapOf("_id" to it + 1), labels = labels)
            val rel = CUDRelationship(op = op, properties = properties, from = start, to = end, rel_type = "MY_REL")
            StreamsSinkEntity(null, rel)
        }

        // when
        val cudQueryStrategy = CUDIngestionStrategy()
        val nodeEvents = cudQueryStrategy.mergeNodeEvents(list)
        val nodeDeleteEvents = cudQueryStrategy.deleteNodeEvents(list)

        val relationshipEvents = cudQueryStrategy.mergeRelationshipEvents(list)
        val relationshipDeleteEvents = cudQueryStrategy.deleteRelationshipEvents(list)

        // then
        assertEquals(emptyList(), nodeEvents)
        assertEquals(emptyList(), nodeDeleteEvents)
        assertEquals(emptyList(), relationshipDeleteEvents)

        assertEquals(3, relationshipEvents.size)
        assertEquals(10, relationshipEvents.map { it.events.size }.sum())
        val createRelFooBar = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MATCH (from) WHERE id(from) = event.from.${CUDIngestionStrategy.ID_KEY}._id
                |MATCH (to) WHERE id(to) = event.to.${CUDIngestionStrategy.ID_KEY}._id
                |CREATE (from)-[r:MY_REL]->(to)
                |SET r = event.properties
            """.trimMargin(), relationshipEvents)
        assertEquals(5, createRelFooBar.events.size)
        val key = "_id"
        assertRelationshipEventsContainsKey(createRelFooBar, key, key)
        val mergeRelFooBar = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MATCH (from) WHERE id(from) = event.from.${CUDIngestionStrategy.ID_KEY}._id
                |MATCH (to) WHERE id(to) = event.to.${CUDIngestionStrategy.ID_KEY}._id
                |MERGE (from)-[r:MY_REL]->(to)
                |SET r += event.properties
            """.trimMargin(), relationshipEvents)
        assertEquals(3, mergeRelFooBar.events.size)
        assertRelationshipEventsContainsKey(mergeRelFooBar, key, key)
        val updateRelFooBar = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MATCH (from) WHERE id(from) = event.from.${CUDIngestionStrategy.ID_KEY}._id
                |MATCH (to) WHERE id(to) = event.to.${CUDIngestionStrategy.ID_KEY}._id
                |MATCH (from)-[r:MY_REL]->(to)
                |SET r += event.properties
            """.trimMargin(), relationshipEvents)
        assertEquals(2, updateRelFooBar.events.size)
        assertRelationshipEventsContainsKey(updateRelFooBar, key, key)
    }

    @Test
    fun `should create, merge, update and delete nodes with compound keys`() {
        // given
        val mergeMarkers = listOf(2, 5, 7)
        val updateMarkers = listOf(3, 6)
        val deleteMarkers = listOf(10)
        val firstKey = "not..... SO SIMPLE!"
        val secondKey = "otherKey"
        val list = (1..10).map {
            val labels = if (it % 2 == 0) listOf("WellBehaved", "C̸r̵a̵z̵y̵ ̶.̵ ̶ ̴ ̸ ̶ ̶ ̵ ̴L̴a̵b̸e̶l") else listOf("WellBehaved", "C̸r̵a̵z̵y̵ ̶.̵ ̶ ̴ ̸ ̶ ̶ ̵ ̴L̴a̵b̸e̶l", "Label")
            val properties = if (it in deleteMarkers) emptyMap() else mapOf("foo" to "foo-value-$it", "id" to it)
            val (op, ids) = when (it) {
                in mergeMarkers -> CUDOperations.merge to mapOf(firstKey to it, secondKey to it)
                in updateMarkers -> CUDOperations.update to mapOf(firstKey to it, secondKey to it)
                in deleteMarkers -> CUDOperations.delete to mapOf(firstKey to it, secondKey to it)
                else -> CUDOperations.create to emptyMap()
            }
            val cudNode = CUDNode(op = op,
                    labels = labels,
                    ids = ids,
                    properties = properties)
            StreamsSinkEntity(null, cudNode)
        }

        // when
        val cudQueryStrategy = CUDIngestionStrategy()
        val nodeEvents = cudQueryStrategy.mergeNodeEvents(list)
        val nodeDeleteEvents = cudQueryStrategy.deleteNodeEvents(list)

        val relationshipEvents = cudQueryStrategy.mergeRelationshipEvents(list)
        val relationshipDeleteEvents = cudQueryStrategy.deleteRelationshipEvents(list)

        // then
        assertEquals(emptyList(), relationshipEvents)
        assertEquals(emptyList(), relationshipDeleteEvents)

        assertEquals(6, nodeEvents.size)
        assertEquals(9, nodeEvents.map { it.events.size }.sum())
        val createNodeFooBar = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |CREATE (n:WellBehaved:`C̸r̵a̵z̵y̵ ̶.̵ ̶ ̴ ̸ ̶ ̶ ̵ ̴L̴a̵b̸e̶l`)
                |SET n = event.properties
            """.trimMargin(), nodeEvents)
        assertEquals(2, createNodeFooBar.events.size)
        val createNodeFooBarLabel = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |CREATE (n:WellBehaved:`C̸r̵a̵z̵y̵ ̶.̵ ̶ ̴ ̸ ̶ ̶ ̵ ̴L̴a̵b̸e̶l`:Label)
                |SET n = event.properties
            """.trimMargin(), nodeEvents)
        assertEquals(2, createNodeFooBarLabel.events.size)

        val mergeNodeFooBar = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MERGE (n:WellBehaved:`C̸r̵a̵z̵y̵ ̶.̵ ̶ ̴ ̸ ̶ ̶ ̵ ̴L̴a̵b̸e̶l` {${firstKey.quote()}: event.${CUDIngestionStrategy.ID_KEY}.${firstKey.quote()}, ${secondKey.quote()}: event.${CUDIngestionStrategy.ID_KEY}.${secondKey.quote()}})
                |SET n += event.properties
            """.trimMargin(), nodeEvents)
        assertEquals(1, mergeNodeFooBar.events.size)
        assertNodeEventsContainsKey(mergeNodeFooBar, firstKey, secondKey)

        val mergeNodeFooBarLabel = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MERGE (n:WellBehaved:`C̸r̵a̵z̵y̵ ̶.̵ ̶ ̴ ̸ ̶ ̶ ̵ ̴L̴a̵b̸e̶l`:Label {${firstKey.quote()}: event.${CUDIngestionStrategy.ID_KEY}.${firstKey.quote()}, ${secondKey.quote()}: event.${CUDIngestionStrategy.ID_KEY}.${secondKey.quote()}})
                |SET n += event.properties
            """.trimMargin(), nodeEvents)
        assertEquals(2, mergeNodeFooBarLabel.events.size)
        assertNodeEventsContainsKey(mergeNodeFooBarLabel, firstKey, secondKey)

        val updateNodeFooBar = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MATCH (n:WellBehaved:`C̸r̵a̵z̵y̵ ̶.̵ ̶ ̴ ̸ ̶ ̶ ̵ ̴L̴a̵b̸e̶l` {${firstKey.quote()}: event.${CUDIngestionStrategy.ID_KEY}.${firstKey.quote()}, ${secondKey.quote()}: event.${CUDIngestionStrategy.ID_KEY}.${secondKey.quote()}})
                |SET n += event.properties
            """.trimMargin(), nodeEvents)
        assertEquals(1, updateNodeFooBar.events.size)
        assertNodeEventsContainsKey(updateNodeFooBar, firstKey, secondKey)

        val updateNodeFooBarLabel = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MATCH (n:WellBehaved:`C̸r̵a̵z̵y̵ ̶.̵ ̶ ̴ ̸ ̶ ̶ ̵ ̴L̴a̵b̸e̶l`:Label {${firstKey.quote()}: event.${CUDIngestionStrategy.ID_KEY}.${firstKey.quote()}, ${secondKey.quote()}: event.${CUDIngestionStrategy.ID_KEY}.${secondKey.quote()}})
                |SET n += event.properties
            """.trimMargin(), nodeEvents)
        assertEquals(1, updateNodeFooBarLabel.events.size)
        assertNodeEventsContainsKey(updateNodeFooBarLabel, firstKey, secondKey)

        assertEquals(1, nodeDeleteEvents.size)
        val nodeDeleteEvent = nodeDeleteEvents.first()
        assertEquals("""
                |${StreamsUtils.UNWIND}
                |MATCH (n:WellBehaved:`C̸r̵a̵z̵y̵ ̶.̵ ̶ ̴ ̸ ̶ ̶ ̵ ̴L̴a̵b̸e̶l` {${firstKey.quote()}: event.${CUDIngestionStrategy.ID_KEY}.${firstKey.quote()}, ${secondKey.quote()}: event.${CUDIngestionStrategy.ID_KEY}.${secondKey.quote()}})
                |DETACH DELETE n
            """.trimMargin(), nodeDeleteEvent.query)
        assertEquals(1, nodeDeleteEvent.events.size)
        assertNodeEventsContainsKey(updateNodeFooBar, firstKey, secondKey)
    }

    @Test
    fun `should create, merge, update and delete nodes without labels`() {
        // given
        val mergeMarkers = listOf(2, 5, 7)
        val updateMarkers = listOf(3, 6)
        val deleteMarkers = listOf(10)
        val key = "key"
        val list = (1..10).map {
            val labels = emptyList<String>()
            val properties = if (it in deleteMarkers) emptyMap() else mapOf("foo" to "foo-value-$it", "id" to it)
            val (op, ids) = when (it) {
                in mergeMarkers -> CUDOperations.merge to mapOf(key to it)
                in updateMarkers -> CUDOperations.update to mapOf(key to it)
                in deleteMarkers -> CUDOperations.delete to mapOf(key to it)
                else -> CUDOperations.create to emptyMap()
            }
            val cudNode = CUDNode(op = op,
                    labels = labels,
                    ids = ids,
                    properties = properties)
            StreamsSinkEntity(null, cudNode)
        }

        // when
        val cudQueryStrategy = CUDIngestionStrategy()
        val nodeEvents = cudQueryStrategy.mergeNodeEvents(list)
        val nodeDeleteEvents = cudQueryStrategy.deleteNodeEvents(list)

        val relationshipEvents = cudQueryStrategy.mergeRelationshipEvents(list)
        val relationshipDeleteEvents = cudQueryStrategy.deleteRelationshipEvents(list)

        // then
        assertEquals(emptyList(), relationshipEvents)
        assertEquals(emptyList(), relationshipDeleteEvents)

        assertEquals(3, nodeEvents.size)
        assertEquals(9, nodeEvents.map { it.events.size }.sum())
        val createNode = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |CREATE (n)
                |SET n = event.properties
            """.trimMargin(), nodeEvents)
        assertEquals(4, createNode.events.size)

        val mergeNode = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MERGE (n {${key.quote()}: event.${CUDIngestionStrategy.ID_KEY}.${key.quote()}})
                |SET n += event.properties
            """.trimMargin(), nodeEvents)
        assertEquals(3, mergeNode.events.size)
        assertNodeEventsContainsKey(mergeNode, key)

        val updateNode = findEventByQuery("""
                |${StreamsUtils.UNWIND}
                |MATCH (n {${key.quote()}: event.${CUDIngestionStrategy.ID_KEY}.${key.quote()}})
                |SET n += event.properties
            """.trimMargin(), nodeEvents)
        assertEquals(2, updateNode.events.size)
        assertNodeEventsContainsKey(updateNode, key)

        assertEquals(1, nodeDeleteEvents.size)
        val nodeDeleteEvent = nodeDeleteEvents.first()
        assertEquals("""
                |${StreamsUtils.UNWIND}
                |MATCH (n {${key.quote()}: event.${CUDIngestionStrategy.ID_KEY}.${key.quote()}})
                |DETACH DELETE n
            """.trimMargin(), nodeDeleteEvent.query)
        assertEquals(1, nodeDeleteEvent.events.size)
        assertNodeEventsContainsKey(updateNode, key)
    }

}