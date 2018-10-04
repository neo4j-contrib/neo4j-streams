package streams

import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class StreamsConfigurationTest {

    @Test
    fun defaultPatterns() {
        val configuration = StreamsConfiguration.from(emptyMap())

        assertEquals(1, configuration.nodeRouting.size)
        assertEquals("neo4j", configuration.nodeRouting[0].topic)
        assertTrue { configuration.nodeRouting[0].all }
        assertTrue { configuration.nodeRouting[0].labels.isEmpty() }

        assertEquals(1, configuration.relRouting.size)
        assertEquals("neo4j", configuration.relRouting[0].topic)
        assertTrue { configuration.relRouting[0].all }
        assertTrue { configuration.relRouting[0].name == "" }
    }

    @Test
    fun nodesTopicName() {
        val configuration = StreamsConfiguration.from(hashMapOf(
                "kafka.routing.nodes.topic1" to "*",
                "kafka.routing.nodes.topic2" to "Label1:Label2{p1, p2}",
                "kafka.routing.nodes.topic3" to "Label1,Label2{p1, p2}",  // 2 rules
                "kafka.routing.nodes.topic4" to "Label2{-p1, -p2}",
                "kafka.routing.nodes.topic5" to "Label3{*}"))

        assertEquals(6, configuration.nodeRouting.size)

        assertEquals("topic1", configuration.nodeRouting[0].topic)
        assertTrue { configuration.nodeRouting[0].all }
        assertTrue { configuration.nodeRouting[0].labels.isEmpty() }
        assertTrue { configuration.nodeRouting[0].include.isEmpty() }
        assertTrue { configuration.nodeRouting[0].exclude.isEmpty() }

        assertEquals("topic2", configuration.nodeRouting[1].topic)
        assertFalse { configuration.nodeRouting[1].all }
        assertEquals(listOf("Label1","Label2"), configuration.nodeRouting[1].labels)
        assertEquals(listOf("p1","p2"), configuration.nodeRouting[1].include)
        assertTrue { configuration.nodeRouting[1].exclude.isEmpty() }

        assertEquals("topic3", configuration.nodeRouting[2].topic)
        assertTrue { configuration.nodeRouting[2].all }
        assertEquals(listOf("Label1"), configuration.nodeRouting[2].labels)
        assertTrue { configuration.nodeRouting[2].include.isEmpty() }
        assertTrue { configuration.nodeRouting[2].exclude.isEmpty() }

        assertEquals("topic3", configuration.nodeRouting[3].topic)
        assertFalse { configuration.nodeRouting[3].all }
        assertEquals(listOf("Label2"), configuration.nodeRouting[3].labels)
        assertEquals(listOf("p1","p2"), configuration.nodeRouting[3].include)
        assertTrue { configuration.nodeRouting[3].exclude.isEmpty() }

        assertEquals("topic4", configuration.nodeRouting[4].topic)
        assertFalse { configuration.nodeRouting[4].all }
        assertTrue { configuration.nodeRouting[4].include.isEmpty() }
        assertEquals(listOf("p1","p2"), configuration.nodeRouting[4].exclude)

        assertEquals("topic5", configuration.nodeRouting[5].topic)
        assertTrue { configuration.nodeRouting[5].all }
        assertTrue { configuration.nodeRouting[5].labels.isEmpty() }
        assertTrue { configuration.nodeRouting[5].include.isEmpty() }
        assertTrue { configuration.nodeRouting[5].exclude.isEmpty() }
    }

    @Test
    fun relsTopicName() {
        val configuration = StreamsConfiguration.from(hashMapOf(
                "kafka.routing.relationships.topic1" to "*",
                "kafka.routing.relationships.topic2" to "KNOWS",
                "kafka.routing.relationships.topic3" to "KNOWS{*}",
                "kafka.routing.relationships.topic4" to "KNOWS,LOVES{p1,p2}", //2 rules
                "kafka.routing.relationships.topic5" to "LOVES{-p1,-p2}"))

        assertEquals(6, configuration.relRouting.size)

        assertEquals("topic1", configuration.relRouting[0].topic)
        assertTrue { configuration.relRouting[0].all }
        assertTrue { configuration.relRouting[0].name == "" }
        assertTrue { configuration.relRouting[0].include.isEmpty() }
        assertTrue { configuration.relRouting[0].exclude.isEmpty() }

        assertEquals("topic2", configuration.relRouting[1].topic)
        assertTrue { configuration.relRouting[1].all }
        assertEquals("KNOWS",configuration.relRouting[1].name)
        assertTrue { configuration.relRouting[1].include.isEmpty() }
        assertTrue { configuration.relRouting[1].exclude.isEmpty() }

        assertEquals("topic3", configuration.relRouting[2].topic)
        assertTrue { configuration.relRouting[2].all }
        assertEquals("KNOWS",configuration.relRouting[2].name)
        assertTrue { configuration.relRouting[2].include.isEmpty() }
        assertTrue { configuration.relRouting[2].exclude.isEmpty() }

        assertEquals("topic4", configuration.relRouting[3].topic)
        assertTrue { configuration.relRouting[3].all }
        assertEquals("KNOWS",configuration.relRouting[3].name)
        assertTrue { configuration.relRouting[3].include.isEmpty() }
        assertTrue { configuration.relRouting[3].exclude.isEmpty() }

        assertEquals("topic4", configuration.relRouting[4].topic)
        assertFalse { configuration.relRouting[4].all }
        assertEquals("LOVES",configuration.relRouting[4].name)
        assertEquals(listOf("p1","p2"),configuration.relRouting[4].include)
        assertTrue { configuration.relRouting[4].exclude.isEmpty() }

        assertEquals("topic5", configuration.relRouting[5].topic)
        assertFalse { configuration.relRouting[5].all }
        assertEquals("LOVES",configuration.relRouting[5].name)
        assertTrue { configuration.relRouting[5].include.isEmpty() }
        assertEquals(listOf("p1","p2"),configuration.relRouting[5].exclude)
    }
}