package streams.kafka.connect.source

import org.apache.kafka.common.config.ConfigException
import org.junit.Test
import streams.kafka.connect.common.Neo4jConnectorConfig
import java.net.URI
import kotlin.test.assertEquals

class Neo4jSourceConnectorConfigTest {

    @Test(expected = ConfigException::class)
    fun `should throw a ConfigException because of unsupported streaming type`() {
        try {
            val originals = mapOf(Neo4jSourceConnectorConfig.SOURCE_TYPE to SourceType.LABELS.toString(),
                Neo4jSourceConnectorConfig.TOPIC to "topic",
                Neo4jSourceConnectorConfig.STREAMING_FROM to StreamingFrom.NOW.toString(),
                Neo4jSourceConnectorConfig.STREAMING_PROPERTY to "timestamp")
            Neo4jSourceConnectorConfig(originals)
        } catch (e: ConfigException) {
            assertEquals("Supported source query types are: ${SourceType.QUERY}", e.message)
            throw e
        }
    }

    @Test(expected = ConfigException::class)
    fun `should throw a ConfigException because of empty query`() {
        try {
            val originals = mapOf(Neo4jSourceConnectorConfig.SOURCE_TYPE to SourceType.QUERY.toString(),
                    Neo4jSourceConnectorConfig.TOPIC to "topic",
                    Neo4jSourceConnectorConfig.STREAMING_FROM to StreamingFrom.NOW.toString(),
                    Neo4jSourceConnectorConfig.STREAMING_PROPERTY to "timestamp")
            Neo4jSourceConnectorConfig(originals)
        } catch (e: ConfigException) {
            assertEquals("You need to define: ${Neo4jSourceConnectorConfig.SOURCE_TYPE_QUERY}", e.message)
            throw e
        }
    }

    @Test
    fun `should return config`() {
        val originals = mapOf(Neo4jSourceConnectorConfig.SOURCE_TYPE to SourceType.QUERY.toString(),
                Neo4jSourceConnectorConfig.SOURCE_TYPE_QUERY to "MATCH (n) RETURN n",
                Neo4jSourceConnectorConfig.TOPIC to "topic",
                Neo4jSourceConnectorConfig.STREAMING_POLL_INTERVAL to "10",
                Neo4jSourceConnectorConfig.STREAMING_FROM to StreamingFrom.NOW.toString(),
                Neo4jSourceConnectorConfig.STREAMING_PROPERTY to "timestamp")
        val config = Neo4jSourceConnectorConfig(originals)
        assertEquals(originals[Neo4jSourceConnectorConfig.TOPIC], config.topic)
        assertEquals(originals[Neo4jSourceConnectorConfig.SOURCE_TYPE_QUERY], config.query)
        assertEquals(originals[Neo4jSourceConnectorConfig.STREAMING_PROPERTY], config.streamingProperty)
        assertEquals(originals[Neo4jSourceConnectorConfig.STREAMING_FROM], config.streamingFrom.toString())
        assertEquals(originals[Neo4jSourceConnectorConfig.STREAMING_POLL_INTERVAL]?.toInt(), config.pollInterval)
    }

    @Test
    fun `should return config null streaming property`() {
        val originals = mapOf(Neo4jSourceConnectorConfig.SOURCE_TYPE to SourceType.QUERY.toString(),
                Neo4jSourceConnectorConfig.SOURCE_TYPE_QUERY to "MATCH (n) RETURN n",
                Neo4jSourceConnectorConfig.TOPIC to "topic",
                Neo4jSourceConnectorConfig.STREAMING_POLL_INTERVAL to "10",
                Neo4jSourceConnectorConfig.STREAMING_FROM to StreamingFrom.NOW.toString())
        val config = Neo4jSourceConnectorConfig(originals)
        assertEquals("", config.streamingProperty)
    }

    @Test
    fun `should return URIs with default port if port does not exist`() {
        val a = "bolt://neo4j.com"
        val b = "bolt://neo4j2.com"

        val originals = mapOf(Neo4jSourceConnectorConfig.SOURCE_TYPE to SourceType.QUERY.toString(),
            Neo4jSourceConnectorConfig.SOURCE_TYPE_QUERY to "MATCH (n) RETURN n",
            Neo4jSourceConnectorConfig.TOPIC to "topic",
            Neo4jSourceConnectorConfig.STREAMING_POLL_INTERVAL to "10",
            Neo4jSourceConnectorConfig.STREAMING_FROM to StreamingFrom.NOW.toString(),
            Neo4jConnectorConfig.SERVER_URI to "$a,$b")
        val config = Neo4jSourceConnectorConfig(originals)

        assertEquals("$a:7687", config.serverUri[0].toString())
        assertEquals("$b:7687", config.serverUri[1].toString())
    }

    @Test
    fun `should parse multiple URIs`() {
        val originals = mapOf(
            Neo4jSourceConnectorConfig.SOURCE_TYPE to SourceType.QUERY.toString(),
            Neo4jSourceConnectorConfig.SOURCE_TYPE_QUERY to "MATCH (n) RETURN n",
            Neo4jSourceConnectorConfig.TOPIC to "topic",
            Neo4jSourceConnectorConfig.STREAMING_POLL_INTERVAL to "10",
            Neo4jSourceConnectorConfig.STREAMING_FROM to StreamingFrom.NOW.toString(),
            Neo4jConnectorConfig.SERVER_URI to "neo4j://192.168.0.1:7687,neo4j://192.168.0.3:7687,neo4j://192.168.0.2"
        )
        val config = Neo4jSourceConnectorConfig(originals)

        assertEquals(
            config.serverUri, listOf(
                URI("neo4j://192.168.0.1:7687"),
                URI("neo4j://192.168.0.3:7687"),
                URI("neo4j://192.168.0.2:7687"),
            )
        )
    }
}