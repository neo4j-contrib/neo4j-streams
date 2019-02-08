package streams.service

import org.junit.Test
import streams.events.*
import kotlin.test.assertEquals

class MergeCDCQueryStrategyTest {

    @Test
    fun `should create the QueryStrategy for mixed events`() {
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
                        start = RelationshipNodeChange(id = "0", labels = listOf("User")),
                        end = RelationshipNodeChange(id = "1", labels = listOf("User")),
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
    fun `should create the QueryStrategy for nodes`() {
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
    }

    @Test
    fun `should create the QueryStrategy for relationships`() {
        // given
        val cdcDataRelationship = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                username = "user",
                txId = 1,
                txEventId = 2,
                txEventsCount = 3,
                operation = OperationType.created
        ),
                payload = RelationshipPayload(
                        id = "2",
                        start = RelationshipNodeChange(id = "0", labels = listOf("User")),
                        end = RelationshipNodeChange(id = "1", labels = listOf("User")),
                        after = RelationshipChange(properties = mapOf("since" to 2014)),
                        before = null,
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
                mapOf("id" to "2", "start" to "0", "end" to "1", "properties" to mapOf("since" to 2014))
        )
        assertEquals(expectedRelEvents, eventsRelList)
    }
}