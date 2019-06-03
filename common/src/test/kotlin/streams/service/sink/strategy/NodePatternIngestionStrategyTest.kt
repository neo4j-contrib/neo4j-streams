package streams.service.sink.strategy

import org.junit.Test
import streams.utils.StreamsUtils
import kotlin.test.assertEquals

class NodePatternIngestionStrategyTest {

    @Test
    fun `should get all properties`() {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id})")
        val strategy = NodePatternIngestionStrategy(config)
        val data = mapOf("id" to 1, "foo" to "foo", "bar" to "bar", "foobar" to "foobar")

        // when
        val queryEvents = strategy.mergeNodeEvents(listOf(data))

        // then
        assertEquals("""
                |${StreamsUtils.UNWIND}
                |MERGE (n:LabelA:LabelB{id: event.keys.id})
                |SET n = event.properties
                |SET n += event.keys
            """.trimMargin(), queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1),
                    "properties" to mapOf("foo" to "foo", "bar" to "bar", "foobar" to "foobar"))
                ),
                queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(emptyList()))
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(emptyList()))
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(emptyList()))
    }

    @Test
    fun `should get nested properties`() {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id, foo.bar})")
        val strategy = NodePatternIngestionStrategy(config)
        val data = mapOf("id" to 1, "foo" to mapOf("bar" to "bar", "foobar" to "foobar"))

        // when
        val queryEvents = strategy.mergeNodeEvents(listOf(data))

        // then
        assertEquals(1, queryEvents.size)
        assertEquals("""
                |${StreamsUtils.UNWIND}
                |MERGE (n:LabelA:LabelB{id: event.keys.id})
                |SET n = event.properties
                |SET n += event.keys
            """.trimMargin(),
            queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1),
                "properties" to mapOf("foo.bar" to "bar"))),
            queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(emptyList()))
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(emptyList()))
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(emptyList()))
    }

    @Test
    fun `should exclude nested properties`() {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id, -foo})")
        val strategy = NodePatternIngestionStrategy(config)
        val data = mapOf("id" to 1, "foo" to mapOf("bar" to "bar", "foobar" to "foobar"), "prop" to 100)

        // when
        val queryEvents = strategy.mergeNodeEvents(listOf(data))

        // then
        assertEquals(1, queryEvents.size)
        assertEquals("""
                |${StreamsUtils.UNWIND}
                |MERGE (n:LabelA:LabelB{id: event.keys.id})
                |SET n = event.properties
                |SET n += event.keys
            """.trimMargin(),
                queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1),
                "properties" to mapOf("prop" to 100))),
                queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(emptyList()))
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(emptyList()))
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(emptyList()))
    }

    @Test
    fun `should include nested properties`() {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id, foo})")
        val strategy = NodePatternIngestionStrategy(config)
        val data = mapOf("id" to 1, "foo" to mapOf("bar" to "bar", "foobar" to "foobar"), "prop" to 100)

        // when
        val queryEvents = strategy.mergeNodeEvents(listOf(data))

        // then
        assertEquals(1, queryEvents.size)
        assertEquals("""
                |${StreamsUtils.UNWIND}
                |MERGE (n:LabelA:LabelB{id: event.keys.id})
                |SET n = event.properties
                |SET n += event.keys
            """.trimMargin(),
                queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1),
                "properties" to mapOf("foo.bar" to "bar", "foo.foobar" to "foobar"))),
                queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(emptyList()))
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(emptyList()))
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(emptyList()))
    }

    @Test
    fun `should exclude the properties`() {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id,-foo,-bar})")
        val strategy = NodePatternIngestionStrategy(config)
        val data = mapOf("id" to 1, "foo" to "foo", "bar" to "bar", "foobar" to "foobar")

        // when
        val queryEvents = strategy.mergeNodeEvents(listOf(data))

        // then
        assertEquals(1, queryEvents.size)
        assertEquals("""
                |${StreamsUtils.UNWIND}
                |MERGE (n:LabelA:LabelB{id: event.keys.id})
                |SET n = event.properties
                |SET n += event.keys
            """.trimMargin(), queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1), "properties" to mapOf("foobar" to "foobar"))), queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(emptyList()))
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(emptyList()))
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(emptyList()))
    }

    @Test
    fun `should include the properties`() {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id,foo,bar})")
        val strategy = NodePatternIngestionStrategy(config)
        val data = mapOf("id" to 1, "foo" to "foo", "bar" to "bar", "foobar" to "foobar")

        // when
        val queryEvents = strategy.mergeNodeEvents(listOf(data))

        // then
        assertEquals("""
                |${StreamsUtils.UNWIND}
                |MERGE (n:LabelA:LabelB{id: event.keys.id})
                |SET n = event.properties
                |SET n += event.keys
            """.trimMargin(), queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1), "properties" to mapOf("foo" to "foo", "bar" to "bar"))), queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(emptyList()))
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(emptyList()))
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(emptyList()))
    }


}