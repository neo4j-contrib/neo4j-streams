package streams


import org.apache.kafka.clients.producer.MockProducer
import org.junit.Test
import org.neo4j.logging.FormattedLog
import org.neo4j.logging.Level
import streams.events.*
import java.io.PrintWriter
import java.io.StringWriter
import kotlin.test.assertEquals

class StreamsEventRouterTest {

    val logger = FormattedLog
            .withUTCTimeZone()
            .withCategory("StreamsEventRouterTest")
            .withLogLevel(Level.DEBUG)
            .toPrintWriter({ PrintWriter(StringWriter()) })

    @Test
    fun shouldSendEventToKafka() {
        // Given
        val streamsEvent = StreamsEventBuilder()
                .withMeta(StreamsEventMetaBuilder()
                        .withOperation(OperationType.created)
                        .withTimestamp(System.currentTimeMillis())
                        .withTransactionEventId(1)
                        .withTransactionEventsCount(1)
                        .withUsername("user")
                        .withTransactionId(1)
                        .build())
                .withSchema(SchemaBuilder().build())
                .withPayload(NodePayloadBuilder()
                        .withBefore(NodeChange(properties = mapOf("prop1" to 1, "prop2" to "pippo", "prop3" to 3), labels = listOf("Label1", "Label2")))
                        .withAfter(NodeChange(properties = mapOf("prop1" to 1, "prop2" to "pippo", "prop3" to 3, "prop4" to 4), labels = listOf("Label1", "Label2")))
                        .build())
                .build()

        // When
        val mockProducer = MockProducer<Long, ByteArray>()
        val router = StreamsEventRouterImpl(log = logger, producer = mockProducer,
                config = StreamsConfiguration.from(mapOf(
                        "kafka.routing.nodes.topic2" to "Label1:Label2{p1,p2}",
                        "kafka.routing.nodes.topic3.1" to "Label1{*}")))
        router.sendEvent(streamsEvent)

        //Then
        assertEquals(1, mockProducer.commitCount())
    }
}