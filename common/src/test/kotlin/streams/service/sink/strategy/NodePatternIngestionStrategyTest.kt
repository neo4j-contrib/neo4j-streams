package streams.service.sink.strategy

import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ArgumentsSource
import org.neo4j.caniuse.Neo4j
import streams.service.StreamsSinkEntity
import streams.utils.StreamsUtils
import kotlin.test.assertEquals

class NodePatternIngestionStrategyTest {

    @ParameterizedTest
    @ArgumentsSource(SupportedVersionsProvider::class)
    fun `should get all properties`(neo4j: Neo4j, expectedPrefix: String) {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id})", true)
        val strategy = NodePatternIngestionStrategy(neo4j, config)
        val data = mapOf("id" to 1, "foo" to "foo", "bar" to "bar", "foobar" to "foobar")

        // when
        val events = listOf(StreamsSinkEntity(data, data))
        val queryEvents = strategy.mergeNodeEvents(events)

        // then
        assertEquals("""
                |${expectedPrefix}${StreamsUtils.UNWIND}
                |MERGE (n:LabelA:LabelB{id: event.keys.id})
                |SET n += event.properties
                |SET n += event.keys
            """.trimMargin(), queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1),
                    "properties" to mapOf("foo" to "foo", "bar" to "bar", "foobar" to "foobar"))
                ),
                queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events))
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events))
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(events))
    }

    @ParameterizedTest
    @ArgumentsSource(SupportedVersionsProvider::class)
    fun `should get nested properties`(neo4j: Neo4j, expectedPrefix: String) {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id, foo.bar})", false)
        val strategy = NodePatternIngestionStrategy(neo4j, config)
        val data = mapOf("id" to 1, "foo" to mapOf("bar" to "bar", "foobar" to "foobar"))

        // when
        val events = listOf(StreamsSinkEntity(data, data))
        val queryEvents = strategy.mergeNodeEvents(events)

        // then
        assertEquals(1, queryEvents.size)
        assertEquals("""
                |${expectedPrefix}${StreamsUtils.UNWIND}
                |MERGE (n:LabelA:LabelB{id: event.keys.id})
                |SET n = event.properties
                |SET n += event.keys
            """.trimMargin(),
            queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1),
                "properties" to mapOf("foo.bar" to "bar"))),
            queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events))
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events))
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(events))
    }

    @ParameterizedTest
    @ArgumentsSource(SupportedVersionsProvider::class)
    fun `should exclude nested properties`(neo4j: Neo4j, expectedPrefix: String) {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id, -foo})", false)
        val strategy = NodePatternIngestionStrategy(neo4j, config)
        val map = mapOf("id" to 1, "foo" to mapOf("bar" to "bar", "foobar" to "foobar"), "prop" to 100)

        // when
        val events = listOf(StreamsSinkEntity(map, map))
        val queryEvents = strategy.mergeNodeEvents(events)

        // then
        assertEquals(1, queryEvents.size)
        assertEquals("""
                |${expectedPrefix}${StreamsUtils.UNWIND}
                |MERGE (n:LabelA:LabelB{id: event.keys.id})
                |SET n = event.properties
                |SET n += event.keys
            """.trimMargin(),
                queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1),
                "properties" to mapOf("prop" to 100))),
                queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events))
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events))
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(events))
    }

    @ParameterizedTest
    @ArgumentsSource(SupportedVersionsProvider::class)
    fun `should include nested properties`(neo4j: Neo4j, expectedPrefix: String) {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id, foo})", true)
        val strategy = NodePatternIngestionStrategy(neo4j, config)
        val data = mapOf("id" to 1, "foo" to mapOf("bar" to "bar", "foobar" to "foobar"), "prop" to 100)

        // when
        val events = listOf(StreamsSinkEntity(data, data))
        val queryEvents = strategy.mergeNodeEvents(events)

        // then
        assertEquals(1, queryEvents.size)
        assertEquals("""
                |${expectedPrefix}${StreamsUtils.UNWIND}
                |MERGE (n:LabelA:LabelB{id: event.keys.id})
                |SET n += event.properties
                |SET n += event.keys
            """.trimMargin(),
                queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1),
                "properties" to mapOf("foo.bar" to "bar", "foo.foobar" to "foobar"))),
                queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events))
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events))
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(events))
    }

    @ParameterizedTest
    @ArgumentsSource(SupportedVersionsProvider::class)
    fun `should exclude the properties`(neo4j: Neo4j, expectedPrefix: String) {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id,-foo,-bar})", false)
        val strategy = NodePatternIngestionStrategy(neo4j, config)
        val data = mapOf("id" to 1, "foo" to "foo", "bar" to "bar", "foobar" to "foobar")

        // when
        val events = listOf(StreamsSinkEntity(data, data))
        val queryEvents = strategy.mergeNodeEvents(events)

        // then
        assertEquals(1, queryEvents.size)
        assertEquals("""
                |${expectedPrefix}${StreamsUtils.UNWIND}
                |MERGE (n:LabelA:LabelB{id: event.keys.id})
                |SET n = event.properties
                |SET n += event.keys
            """.trimMargin(), queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1), "properties" to mapOf("foobar" to "foobar"))), queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events))
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events))
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(events))
    }

    @ParameterizedTest
    @ArgumentsSource(SupportedVersionsProvider::class)
    fun `should include the properties`(neo4j: Neo4j, expectedPrefix: String) {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id,foo,bar})", false)
        val strategy = NodePatternIngestionStrategy(neo4j, config)
        val data = mapOf("id" to 1, "foo" to "foo", "bar" to "bar", "foobar" to "foobar")

        // when
        val events = listOf(StreamsSinkEntity(data, data))
        val queryEvents = strategy.mergeNodeEvents(events)

        // then
        assertEquals("""
                |${expectedPrefix}${StreamsUtils.UNWIND}
                |MERGE (n:LabelA:LabelB{id: event.keys.id})
                |SET n = event.properties
                |SET n += event.keys
            """.trimMargin(), queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1), "properties" to mapOf("foo" to "foo", "bar" to "bar"))), queryEvents[0].events)
        assertEquals(emptyList(), strategy.deleteNodeEvents(events))
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events))
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(events))
    }

    @ParameterizedTest
    @ArgumentsSource(SupportedVersionsProvider::class)
    fun `should delete the node`(neo4j: Neo4j, expectedPrefix: String) {
        // given
        val config = NodePatternConfiguration.parse("(:LabelA:LabelB{!id})", true)
        val strategy = NodePatternIngestionStrategy(neo4j, config)
        val data = mapOf("id" to 1, "foo" to "foo", "bar" to "bar", "foobar" to "foobar")

        // when
        val events = listOf(StreamsSinkEntity(data, null))
        val queryEvents = strategy.deleteNodeEvents(events)

        // then
        assertEquals("""
                |${expectedPrefix}${StreamsUtils.UNWIND}
                |MATCH (n:LabelA:LabelB{id: event.keys.id})
                |DETACH DELETE n
            """.trimMargin(), queryEvents[0].query)
        assertEquals(listOf(mapOf("keys" to mapOf("id" to 1))),
                queryEvents[0].events)
        assertEquals(emptyList(), strategy.mergeNodeEvents(events))
        assertEquals(emptyList(), strategy.deleteRelationshipEvents(events))
        assertEquals(emptyList(), strategy.mergeRelationshipEvents(events))
    }

}