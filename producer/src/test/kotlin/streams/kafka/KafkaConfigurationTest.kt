package streams.kafka

import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class KafkaConfigurationTest {

    @Test
    fun shouldCreateDefaultConfiguration() {
        val configuration = KafkaConfiguration.from(emptyMap())

        assertEquals(1, configuration.nodeRouting.size)
        assertEquals("neo4j", configuration.nodeRouting[0].topic)
        assertTrue { configuration.nodeRouting[0].all }
        assertTrue { configuration.nodeRouting[0].labels.isEmpty() }

        assertEquals(1, configuration.relRouting.size)
        assertEquals("neo4j", configuration.relRouting[0].topic)
        assertTrue { configuration.relRouting[0].all }
        assertTrue { configuration.relRouting[0].name == "" }
    }
}