package streams.service

import org.junit.Test
import org.neo4j.graphdb.schema.ConstraintType
import streams.events.*
import kotlin.test.assertEquals

class MergeCDCQueryStrategyTest {

    @Test
    fun `should create the Merge Query Strategy for mixed events`() {
        // given
        val cdcDataStart = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                username = "user",
                txId = 1,
                txEventId = 0,
                txEventsCount = 3,
                operation = OperationType.created
        ),
                payload = NodePayload(id = "0",
                        before = null,
                        after = NodeChange(properties = mapOf("name" to "Andrea", "comp@ny" to "LARUS-BA"), labels = listOf("User"))
                ),
                schema = Schema()
        )
        val cdcDataEnd = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                username = "user",
                txId = 1,
                txEventId = 1,
                txEventsCount = 3,
                operation = OperationType.created
        ),
                payload = NodePayload(id = "1",
                        before = null,
                        after = NodeChange(properties = mapOf("name" to "Michael", "comp@ny" to "Neo4j"), labels = listOf("User"))
                ),
                schema = Schema()
        )
        val cdcDataRelationship = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                username = "user",
                txId = 1,
                txEventId = 2,
                txEventsCount = 3,
                operation = OperationType.created
        ),
                payload = RelationshipPayload(
                        id = "2",
                        start = RelationshipNodeChange(id = "0", labels = listOf("User"), ids = emptyMap()),
                        end = RelationshipNodeChange(id = "1", labels = listOf("User"), ids = emptyMap()),
                        after = RelationshipChange(properties = mapOf("since" to 2014)),
                        before = null,
                        label = "KNOWS WHO"
                ),
                schema = Schema()
        )
        val cdcQueryStrategy = MergeCDCQueryStrategy()
        val txEvents = listOf(cdcDataStart, cdcDataEnd, cdcDataRelationship)

        // when
        val nodeEvents = cdcQueryStrategy.mergeNodeEvents(txEvents)
        val nodeDeleteEvents = cdcQueryStrategy.deleteNodeEvents(txEvents)

        val relationshipEvents = cdcQueryStrategy.mergeRelationshipEvents(txEvents)
        val relationshipDeleteEvents = cdcQueryStrategy.deleteRelationshipEvents(txEvents)

        // then
        assertEquals(0, nodeDeleteEvents.size)
        assertEquals(1, nodeEvents.size)
        val nodeQuery = nodeEvents[0].query
        val expectedNodeQuery = """
            |UNWIND {events} AS event
            |MERGE (n:StreamsEvent{streams_id: event.id})
            |SET n = event.properties
            |SET n.streams_id = event.id
            |SET n:`User`
        """.trimMargin()
        assertEquals(expectedNodeQuery, nodeQuery.trimIndent())
        val eventsNodeList = nodeEvents[0].events
        assertEquals(2, eventsNodeList.size)
        val expectedNodeEvents = listOf(
                mapOf("id" to "0", "properties" to mapOf("name" to "Andrea", "comp@ny" to "LARUS-BA")),
                mapOf("id" to "1", "properties" to mapOf("name" to "Michael", "comp@ny" to "Neo4j"))
        )
        assertEquals(expectedNodeEvents, eventsNodeList)

        assertEquals(0, relationshipDeleteEvents.size)
        assertEquals(1, relationshipEvents.size)
        val relQuery = relationshipEvents[0].query
        val expectedRelQuery = """
            |UNWIND {events} AS event
            |MERGE (start:StreamsEvent{streams_id: event.start})
            |MERGE (end:StreamsEvent{streams_id: event.end})
            |MERGE (start)-[r:`KNOWS WHO`{streams_id: event.id}]->(end)
            |SET r = event.properties
            |SET r.streams_id = event.id
        """.trimMargin()
        assertEquals(expectedRelQuery, relQuery.trimIndent())
        val eventsRelList = relationshipEvents[0].events
        assertEquals(1, eventsRelList.size)
        val expectedRelEvents = listOf(
                mapOf("id" to "2", "start" to "0", "end" to "1", "properties" to mapOf("since" to 2014))
        )
        assertEquals(expectedRelEvents, eventsRelList)
    }

    @Test
    fun `should create the Merge Query Strategy for node updates`() {
        // given
        val nodeSchema = Schema()
        // given
        val cdcDataStart = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                username = "user",
                txId = 1,
                txEventId = 0,
                txEventsCount = 3,
                operation = OperationType.updated
        ),
                payload = NodePayload(id = "0",
                        before = NodeChange(properties = mapOf("name" to "Andrea", "surname" to "Santurbano"), labels = listOf("User", "ToRemove")),
                        after = NodeChange(properties = mapOf("name" to "Andrea", "surname" to "Santurbano", "comp@ny" to "LARUS-BA"), labels = listOf("User", "NewLabel"))
                ),
                schema = nodeSchema
        )
        val cdcDataEnd = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                username = "user",
                txId = 1,
                txEventId = 1,
                txEventsCount = 3,
                operation = OperationType.updated
        ),
                payload = NodePayload(id = "1",
                        before = NodeChange(properties = mapOf("name" to "Michael", "surname" to "Hunger"), labels = listOf("User", "ToRemove")),
                        after = NodeChange(properties = mapOf("name" to "Michael", "surname" to "Hunger", "comp@ny" to "Neo4j"), labels = listOf("User", "NewLabel"))
                ),
                schema = nodeSchema
        )
        val cdcQueryStrategy = MergeCDCQueryStrategy()
        val txEvents = listOf(cdcDataStart, cdcDataEnd)

        // when
        val nodeEvents = cdcQueryStrategy.mergeNodeEvents(txEvents)
        val nodeDeleteEvents = cdcQueryStrategy.deleteNodeEvents(txEvents)

        // then
        assertEquals(0, nodeDeleteEvents.size)
        assertEquals(1, nodeEvents.size)
        val nodeQuery = nodeEvents[0].query
        val expectedNodeQuery = """
            |UNWIND {events} AS event
            |MERGE (n:StreamsEvent{streams_id: event.id})
            |SET n = event.properties
            |SET n.streams_id = event.id
            |SET n:`NewLabel`
            |REMOVE n:`ToRemove`
        """.trimMargin()
        assertEquals(expectedNodeQuery, nodeQuery.trimIndent())
        val eventsNodeList = nodeEvents[0].events
        assertEquals(2, eventsNodeList.size)
        val expectedNodeEvents = listOf(
                mapOf("id" to "0", "properties" to mapOf("name" to "Andrea", "surname" to "Santurbano", "comp@ny" to "LARUS-BA")),
                mapOf("id" to "1", "properties" to mapOf("name" to "Michael", "surname" to "Hunger", "comp@ny" to "Neo4j"))
        )
        assertEquals(expectedNodeEvents, eventsNodeList)
    }

    @Test
    fun `should create the Merge Query Strategy for relationships updates`() {
        // given
        val cdcDataRelationship = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                username = "user",
                txId = 1,
                txEventId = 2,
                txEventsCount = 3,
                operation = OperationType.updated
        ),
                payload = RelationshipPayload(
                        id = "2",
                        start = RelationshipNodeChange(id = "0", labels = listOf("User"), ids = emptyMap()),
                        end = RelationshipNodeChange(id = "1", labels = listOf("User"), ids = emptyMap()),
                        after = RelationshipChange(properties = mapOf("since" to 2014, "foo" to "label")),
                        before = RelationshipChange(properties = mapOf("since" to 2014)),
                        label = "KNOWS WHO"
                ),
                schema = Schema()
        )
        val cdcQueryStrategy = MergeCDCQueryStrategy()
        val txEvents = listOf(cdcDataRelationship)

        // when
        val relationshipEvents = cdcQueryStrategy.mergeRelationshipEvents(txEvents)
        val relationshipDeleteEvents = cdcQueryStrategy.deleteRelationshipEvents(txEvents)

        // then
        assertEquals(0, relationshipDeleteEvents.size)
        assertEquals(1, relationshipEvents.size)
        val relQuery = relationshipEvents[0].query
        val expectedRelQuery = """
            |UNWIND {events} AS event
            |MERGE (start:StreamsEvent{streams_id: event.start})
            |MERGE (end:StreamsEvent{streams_id: event.end})
            |MERGE (start)-[r:`KNOWS WHO`{streams_id: event.id}]->(end)
            |SET r = event.properties
            |SET r.streams_id = event.id
        """.trimMargin()
        assertEquals(expectedRelQuery, relQuery.trimIndent())
        val eventsRelList = relationshipEvents[0].events
        assertEquals(1, eventsRelList.size)
        val expectedRelEvents = listOf(
                mapOf("id" to "2", "start" to "0", "end" to "1", "properties" to mapOf("since" to 2014, "foo" to "label"))
        )
        assertEquals(expectedRelEvents, eventsRelList)
    }

    @Test
    fun `should create the Merge Query Strategy for node deletes`() {
        // given
        val nodeSchema = Schema()
        // given
        val cdcDataStart = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                username = "user",
                txId = 1,
                txEventId = 0,
                txEventsCount = 3,
                operation = OperationType.deleted
        ),
                payload = NodePayload(id = "0",
                        before = NodeChange(properties = mapOf("name" to "Andrea", "surname" to "Santurbano"), labels = listOf("User")),
                        after = null
                ),
                schema = nodeSchema
        )
        val cdcDataEnd = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                username = "user",
                txId = 1,
                txEventId = 1,
                txEventsCount = 3,
                operation = OperationType.deleted
        ),
                payload = NodePayload(id = "1",
                        before = NodeChange(properties = mapOf("name" to "Michael", "surname" to "Hunger"), labels = listOf("User")),
                        after = null
                ),
                schema = nodeSchema
        )
        val cdcQueryStrategy = MergeCDCQueryStrategy()
        val txEvents = listOf(cdcDataStart, cdcDataEnd)

        // when
        val nodeEvents = cdcQueryStrategy.mergeNodeEvents(txEvents)
        val nodeDeleteEvents = cdcQueryStrategy.deleteNodeEvents(txEvents)

        // then
        assertEquals(1, nodeDeleteEvents.size)
        assertEquals(0, nodeEvents.size)
        val nodeQuery = nodeDeleteEvents[0].query
        val expectedNodeQuery = """
            |UNWIND {events} AS event MATCH (n:StreamsEvent{streams_id: event.id}) DETACH DELETE n
        """.trimMargin()
        assertEquals(expectedNodeQuery, nodeQuery.trimIndent())
        val eventsNodeList = nodeDeleteEvents[0].events
        assertEquals(2, eventsNodeList.size)
        val expectedNodeEvents = listOf(
                mapOf("id" to "0"),
                mapOf("id" to "1")
        )
        assertEquals(expectedNodeEvents, eventsNodeList)
    }

    @Test
    fun `should create the Merge Query Strategy for relationships deletes`() {
        // given
        val cdcDataRelationship = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                username = "user",
                txId = 1,
                txEventId = 2,
                txEventsCount = 3,
                operation = OperationType.deleted
        ),
                payload = RelationshipPayload(
                        id = "2",
                        start = RelationshipNodeChange(id = "0", labels = listOf("User"), ids = emptyMap()),
                        end = RelationshipNodeChange(id = "1", labels = listOf("User"), ids = emptyMap()),
                        after = RelationshipChange(properties = mapOf("since" to 2014, "foo" to "label")),
                        before = RelationshipChange(properties = mapOf("since" to 2014)),
                        label = "KNOWS WHO"
                ),
                schema = Schema()
        )
        val cdcQueryStrategy = MergeCDCQueryStrategy()
        val txEvents = listOf(cdcDataRelationship)

        // when
        val relationshipEvents = cdcQueryStrategy.mergeRelationshipEvents(txEvents)
        val relationshipDeleteEvents = cdcQueryStrategy.deleteRelationshipEvents(txEvents)

        // then
        assertEquals(1, relationshipDeleteEvents.size)
        assertEquals(0, relationshipEvents.size)
        val relQuery = relationshipDeleteEvents[0].query
        val expectedRelQuery = """
            |UNWIND {events} AS event MATCH ()-[r:`KNOWS WHO`{streams_id: event.id}]-() DELETE r
        """.trimMargin()
        assertEquals(expectedRelQuery, relQuery.trimIndent())
        val eventsRelList = relationshipDeleteEvents[0].events
        assertEquals(1, eventsRelList.size)
        val expectedRelEvents = listOf(mapOf("id" to "2"))
        assertEquals(expectedRelEvents, eventsRelList)
    }

}

class SchemaCDCQueryStrategyTest {

    @Test
    fun `should create the Schema Query Strategy for mixed events`() {
        // given
        val nodeSchema = Schema(properties = mapOf("name" to "String", "surname" to "String", "comp@ny" to "String"),
                constraints = listOf(Constraint(label = "User", type = ConstraintType.NODE_KEY, properties = setOf("name", "surname"))))
        val cdcDataStart = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                username = "user",
                txId = 1,
                txEventId = 0,
                txEventsCount = 3,
                operation = OperationType.created
        ),
                payload = NodePayload(id = "0",
                        before = null,
                        after = NodeChange(properties = mapOf("name" to "Andrea", "surname" to "Santurbano", "comp@ny" to "LARUS-BA"), labels = listOf("User"))
                ),
                schema = nodeSchema
        )
        val cdcDataEnd = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                username = "user",
                txId = 1,
                txEventId = 1,
                txEventsCount = 3,
                operation = OperationType.created
        ),
                payload = NodePayload(id = "1",
                        before = null,
                        after = NodeChange(properties = mapOf("name" to "Michael", "surname" to "Hunger", "comp@ny" to "Neo4j"), labels = listOf("User"))
                ),
                schema = nodeSchema
        )
        val cdcDataRelationship = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                username = "user",
                txId = 1,
                txEventId = 2,
                txEventsCount = 3,
                operation = OperationType.created
        ),
                payload = RelationshipPayload(
                        id = "2",
                        start = RelationshipNodeChange(id = "0", labels = listOf("User"), ids = mapOf("name" to "Andrea", "surname" to "Santurbano")),
                        end = RelationshipNodeChange(id = "1", labels = listOf("User"), ids = mapOf("name" to "Michael", "surname" to "Hunger")),
                        after = RelationshipChange(properties = mapOf("since" to 2014)),
                        before = null,
                        label = "KNOWS WHO"
                ),
                schema = Schema()
        )
        val cdcQueryStrategy = SchemaCDCQueryStrategy()
        val txEvents = listOf(cdcDataStart, cdcDataEnd, cdcDataRelationship)

        // when
        val nodeEvents = cdcQueryStrategy.mergeNodeEvents(txEvents)
        val nodeDeleteEvents = cdcQueryStrategy.deleteNodeEvents(txEvents)

        val relationshipEvents = cdcQueryStrategy.mergeRelationshipEvents(txEvents)
        val relationshipDeleteEvents = cdcQueryStrategy.deleteRelationshipEvents(txEvents)

        // then
        assertEquals(0, nodeDeleteEvents.size)
        assertEquals(1, nodeEvents.size)
        val nodeQuery = nodeEvents[0].query
        val expectedNodeQuery = """
            |UNWIND {events} AS event
            |MERGE (n:`User`{`name`: event.properties.`name`, `surname`: event.properties.`surname`})
            |SET n = event.properties
            |
        """.trimMargin()
        assertEquals(expectedNodeQuery, nodeQuery.trimIndent())
        val eventsNodeList = nodeEvents[0].events
        assertEquals(2, eventsNodeList.size)
        val expectedNodeEvents = listOf(
                mapOf("properties" to mapOf("name" to "Andrea", "surname" to "Santurbano", "comp@ny" to "LARUS-BA")),
                mapOf("properties" to mapOf("name" to "Michael", "surname" to "Hunger", "comp@ny" to "Neo4j"))
        )
        assertEquals(expectedNodeEvents, eventsNodeList)

        assertEquals(0, relationshipDeleteEvents.size)
        assertEquals(1, relationshipEvents.size)
        val relQuery = relationshipEvents[0].query
        val expectedRelQuery = """
            |UNWIND {events} AS event
            |MERGE (start:`User`{`name`: event.start.`name`, `surname`: event.start.`surname`})
            |MERGE (end:`User`{`name`: event.end.`name`, `surname`: event.end.`surname`})
            |MERGE (start)-[r:`KNOWS WHO`]->(end)
            |SET r = event.properties
        """.trimMargin()
        assertEquals(expectedRelQuery, relQuery.trimIndent())
        val eventsRelList = relationshipEvents[0].events
        assertEquals(1, eventsRelList.size)
        val expectedRelEvents = listOf(
                mapOf("start" to mapOf("name" to "Andrea", "surname" to "Santurbano"),
                        "end" to mapOf("name" to "Michael", "surname" to "Hunger"), "properties" to mapOf("since" to 2014))
        )
        assertEquals(expectedRelEvents, eventsRelList)
    }

    @Test
    fun `should create the Schema Query Strategy for nodes`() {
        // given
        val nodeSchema = Schema(properties = mapOf("name" to "String", "surname" to "String", "comp@ny" to "String"),
                constraints = listOf(Constraint(label = "User", type = ConstraintType.NODE_KEY, properties = setOf("name", "surname"))))
        val cdcDataStart = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                username = "user",
                txId = 1,
                txEventId = 0,
                txEventsCount = 3,
                operation = OperationType.updated
        ),
                payload = NodePayload(id = "0",
                        before = NodeChange(properties = mapOf("name" to "Andrea", "surname" to "Santurbano"), labels = listOf("User", "ToRemove")),
                        after = NodeChange(properties = mapOf("name" to "Andrea", "surname" to "Santurbano", "comp@ny" to "LARUS-BA"), labels = listOf("User", "NewLabel"))
                ),
                schema = nodeSchema
        )
        val cdcDataEnd = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                username = "user",
                txId = 1,
                txEventId = 1,
                txEventsCount = 3,
                operation = OperationType.updated
        ),
                payload = NodePayload(id = "1",
                        before = NodeChange(properties = mapOf("name" to "Michael", "surname" to "Hunger"), labels = listOf("User", "ToRemove")),
                        after = NodeChange(properties = mapOf("name" to "Michael", "surname" to "Hunger", "comp@ny" to "Neo4j"), labels = listOf("User", "NewLabel"))
                ),
                schema = nodeSchema
        )
        val cdcQueryStrategy = SchemaCDCQueryStrategy()
        val txEvents = listOf(cdcDataStart, cdcDataEnd)

        // when
        val nodeEvents = cdcQueryStrategy.mergeNodeEvents(txEvents)
        val nodeDeleteEvents = cdcQueryStrategy.deleteNodeEvents(txEvents)

        // then
        assertEquals(0, nodeDeleteEvents.size)
        assertEquals(1, nodeEvents.size)
        val nodeQuery = nodeEvents[0].query
        val expectedNodeQuery = """
            |UNWIND {events} AS event
            |MERGE (n:`User`{`name`: event.properties.`name`, `surname`: event.properties.`surname`})
            |SET n = event.properties
            |SET n:`NewLabel`
            |REMOVE n:`ToRemove`
        """.trimMargin()
        assertEquals(expectedNodeQuery, nodeQuery.trimIndent())
        val eventsNodeList = nodeEvents[0].events
        assertEquals(2, eventsNodeList.size)
        val expectedNodeEvents = listOf(
                mapOf("properties" to mapOf("name" to "Andrea", "surname" to "Santurbano", "comp@ny" to "LARUS-BA")),
                mapOf("properties" to mapOf("name" to "Michael", "surname" to "Hunger", "comp@ny" to "Neo4j"))
        )
        assertEquals(expectedNodeEvents, eventsNodeList)
    }

    @Test
    fun `should create the Schema Query Strategy for relationships`() {
        // given
        val cdcDataRelationship = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                username = "user",
                txId = 1,
                txEventId = 2,
                txEventsCount = 3,
                operation = OperationType.updated
        ),
                payload = RelationshipPayload(
                        id = "2",
                        start = RelationshipNodeChange(id = "0", labels = listOf("User"), ids = mapOf("name" to "Andrea", "surname" to "Santurbano")),
                        end = RelationshipNodeChange(id = "1", labels = listOf("User"), ids = mapOf("name" to "Michael", "surname" to "Hunger")),
                        after = RelationshipChange(properties = mapOf("since" to 2014)),
                        before = null,
                        label = "KNOWS WHO"
                ),
                schema = Schema()
        )
        val cdcQueryStrategy = SchemaCDCQueryStrategy()
        val txEvents = listOf(cdcDataRelationship)

        // when
        val relationshipEvents = cdcQueryStrategy.mergeRelationshipEvents(txEvents)
        val relationshipDeleteEvents = cdcQueryStrategy.deleteRelationshipEvents(txEvents)

        // then
        assertEquals(0, relationshipDeleteEvents.size)
        assertEquals(1, relationshipEvents.size)
        val relQuery = relationshipEvents[0].query
        val expectedRelQuery = """
            |UNWIND {events} AS event
            |MERGE (start:`User`{`name`: event.start.`name`, `surname`: event.start.`surname`})
            |MERGE (end:`User`{`name`: event.end.`name`, `surname`: event.end.`surname`})
            |MERGE (start)-[r:`KNOWS WHO`]->(end)
            |SET r = event.properties
        """.trimMargin()
        assertEquals(expectedRelQuery, relQuery.trimIndent())
        val eventsRelList = relationshipEvents[0].events
        assertEquals(1, eventsRelList.size)
        val expectedRelEvents = listOf(
                mapOf("start" to mapOf("name" to "Andrea", "surname" to "Santurbano"),
                        "end" to mapOf("name" to "Michael", "surname" to "Hunger"),
                        "properties" to mapOf("since" to 2014))
        )
        assertEquals(expectedRelEvents, eventsRelList)
    }

    @Test
    fun `should create the Schema Query Strategy for node deletes`() {
        // given
        val nodeSchema = Schema(properties = mapOf("name" to "String", "surname" to "String", "comp@ny" to "String"),
                constraints = listOf(Constraint(label = "User", type = ConstraintType.NODE_KEY, properties = setOf("name", "surname"))))
        val cdcDataStart = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                username = "user",
                txId = 1,
                txEventId = 0,
                txEventsCount = 3,
                operation = OperationType.deleted
        ),
                payload = NodePayload(id = "0",
                        before = NodeChange(properties = mapOf("name" to "Andrea", "surname" to "Santurbano", "comp@ny" to "LARUS-BA"), labels = listOf("User")),
                        after = null
                ),
                schema = nodeSchema
        )
        val cdcDataEnd = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                username = "user",
                txId = 1,
                txEventId = 1,
                txEventsCount = 3,
                operation = OperationType.deleted
        ),
                payload = NodePayload(id = "1",
                        before = NodeChange(properties = mapOf("name" to "Michael", "surname" to "Hunger", "comp@ny" to "Neo4j"), labels = listOf("User")),
                        after = null
                ),
                schema = nodeSchema
        )
        val cdcQueryStrategy = SchemaCDCQueryStrategy()
        val txEvents = listOf(cdcDataStart, cdcDataEnd)

        // when
        val nodeEvents = cdcQueryStrategy.mergeNodeEvents(txEvents)
        val nodeDeleteEvents = cdcQueryStrategy.deleteNodeEvents(txEvents)

        // then
        assertEquals(1, nodeDeleteEvents.size)
        assertEquals(0, nodeEvents.size)
        val nodeQuery = nodeDeleteEvents[0].query
        val expectedNodeQuery = """
            |UNWIND {events} AS event
            |MATCH (n:`User`{`name`: event.properties.`name`, `surname`: event.properties.`surname`})
            |DETACH DELETE n
        """.trimMargin()
        assertEquals(expectedNodeQuery, nodeQuery.trimIndent())
        val eventsNodeList = nodeDeleteEvents[0].events
        assertEquals(2, eventsNodeList.size)
        val expectedNodeEvents = listOf(
                mapOf("properties" to mapOf("name" to "Andrea", "surname" to "Santurbano", "comp@ny" to "LARUS-BA")),
                mapOf("properties" to mapOf("name" to "Michael", "surname" to "Hunger", "comp@ny" to "Neo4j"))
        )
        assertEquals(expectedNodeEvents, eventsNodeList)
    }

    @Test
    fun `should create the Schema Query Strategy for relationships deletes`() {
        // given
        val cdcDataRelationship = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                username = "user",
                txId = 1,
                txEventId = 2,
                txEventsCount = 3,
                operation = OperationType.deleted
        ),
                payload = RelationshipPayload(
                        id = "2",
                        start = RelationshipNodeChange(id = "0", labels = listOf("User"), ids = mapOf("name" to "Andrea", "surname" to "Santurbano")),
                        end = RelationshipNodeChange(id = "1", labels = listOf("User"), ids = mapOf("name" to "Michael", "surname" to "Hunger")),
                        after = RelationshipChange(properties = mapOf("since" to 2014, "foo" to "label")),
                        before = RelationshipChange(properties = mapOf("since" to 2014)),
                        label = "KNOWS WHO"
                ),
                schema = Schema()
        )
        val cdcQueryStrategy = SchemaCDCQueryStrategy()
        val txEvents = listOf(cdcDataRelationship)

        // when
        val relationshipEvents = cdcQueryStrategy.mergeRelationshipEvents(txEvents)
        val relationshipDeleteEvents = cdcQueryStrategy.deleteRelationshipEvents(txEvents)

        // then
        assertEquals(1, relationshipDeleteEvents.size)
        assertEquals(0, relationshipEvents.size)
        val relQuery = relationshipDeleteEvents[0].query
        val expectedRelQuery = """
            |UNWIND {events} AS event
            |MATCH (start:`User`{`name`: event.start.`name`, `surname`: event.start.`surname`})
            |MATCH (end:`User`{`name`: event.end.`name`, `surname`: event.end.`surname`})
            |MATCH (start)-[r:`KNOWS WHO`]->(end)
            |DELETE r
        """.trimMargin()
        assertEquals(expectedRelQuery, relQuery.trimIndent())
        val eventsRelList = relationshipDeleteEvents[0].events
        assertEquals(1, eventsRelList.size)
        val expectedRelEvents = listOf(
                mapOf("start" to mapOf("name" to "Andrea", "surname" to "Santurbano"),
                        "end" to mapOf("name" to "Michael", "surname" to "Hunger"))
        )
        assertEquals(expectedRelEvents, eventsRelList)
    }

}