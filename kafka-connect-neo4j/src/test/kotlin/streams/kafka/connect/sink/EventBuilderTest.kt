package streams.kafka.connect.sink

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.Timestamp
import org.apache.kafka.connect.sink.SinkRecord
import org.junit.Test
import java.util.*
import kotlin.test.assertEquals

class EventBuilderTest {
    private val PERSON_SCHEMA = SchemaBuilder.struct().name("com.example.Person")
            .field("firstName", Schema.STRING_SCHEMA)
            .field("lastName", Schema.STRING_SCHEMA)
            .field("age", Schema.OPTIONAL_INT32_SCHEMA)
            .field("bool", Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field("short", Schema.OPTIONAL_INT16_SCHEMA)
            .field("byte", Schema.OPTIONAL_INT8_SCHEMA)
            .field("long", Schema.OPTIONAL_INT64_SCHEMA)
            .field("float", Schema.OPTIONAL_FLOAT32_SCHEMA)
            .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field("modified", Timestamp.SCHEMA)
            .build()

    @Test
    fun `should create event map properly`() {
        // Given
        val firstTopic = "neotopic"
        val secondTopic = "foo"
        val batchSize = 2
        val struct= Struct(PERSON_SCHEMA)
                .put("firstName", "Alex")
                .put("lastName", "Smith")
                .put("bool", true)
                .put("short", 1234.toShort())
                .put("byte", (-32).toByte())
                .put("long", 12425436L)
                .put("float", 2356.3.toFloat())
                .put("double", -2436546.56457)
                .put("age", 21)
                .put("modified", Date(1474661402123L))
        val input = listOf(SinkRecord(firstTopic, 1, null, null, PERSON_SCHEMA, struct, 42),
                SinkRecord(firstTopic, 1, null, null, PERSON_SCHEMA, struct, 42),
                SinkRecord(firstTopic, 1, null, null, PERSON_SCHEMA, struct, 43),
                SinkRecord(firstTopic, 1, null, null, PERSON_SCHEMA, struct, 44),
                SinkRecord(firstTopic, 1, null, null, PERSON_SCHEMA, struct, 45),
                SinkRecord(secondTopic, 1, null, null, PERSON_SCHEMA, struct, 43)) // 5 records for topic "neotopic", 1 for topic "foo"
        val topics = listOf(firstTopic, secondTopic)

        // When
        val data = EventBuilder()
                .withBatchSize(batchSize)
                .withTopics(topics)
                .withSinkRecords(input)
                .build()

        // Then
        assertEquals(topics, data.keys.toList())
        assertEquals(3, data[firstTopic]!!.size) // n° of chunks for "neotopic"
        assertEquals(1, data[secondTopic]!!.size) // n° of chunks for "foo"
    }
}