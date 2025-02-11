package integrations.kafka

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.kafka.clients.producer.ProducerRecord
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.hamcrest.Matchers
import org.junit.Test
import org.junit.jupiter.api.assertThrows
import org.neo4j.cypher.internal.parser.javacc.ParseExceptions.expected
import org.neo4j.function.ThrowingSupplier
import org.neo4j.graphdb.Node
import streams.Assert
import streams.Assert.withDoublePrecision
import streams.setConfig
import streams.start
import java.util.UUID
import java.util.concurrent.TimeUnit


class KafkaEventSinkAvroTSE : KafkaEventSinkBaseTSE() {

    @Test
    fun `should insert AVRO data`() {
        //given
        val topic = "avro"
        db.setConfig(
            "streams.sink.topic.cypher.$topic",
            "CREATE (p:Place{name: event.name, coordinates: event.coordinates, citizens: event.citizens})"
        )
        db.setConfig("kafka.key.deserializer", KafkaAvroDeserializer::class.java.name)
        db.setConfig("kafka.value.deserializer", KafkaAvroDeserializer::class.java.name)
        db.setConfig("kafka.schema.registry.url", KafkaEventSinkSuiteIT.schemaRegistry.getSchemaRegistryUrl())
        db.start()

        val PLACE_SCHEMA = SchemaBuilder.builder("com.namespace")
            .record("Place").fields()
            .name("name").type().stringType().noDefault()
            .name("coordinates").type().array().items().doubleType().noDefault()
            .name("citizens").type().longType().noDefault()
            .endRecord()
        val coordinates = listOf(42.30000, -11.22222)
        val citizens = 1_000_000L
        val struct = GenericRecordBuilder(PLACE_SCHEMA)
            .set("name", "Foo")
            .set("coordinates", coordinates)
            .set("citizens", citizens)
            .build()

        // when
        kafkaAvroProducer.send(ProducerRecord<GenericRecord, GenericRecord>(topic, null, struct)).get()

        // then
        val expected = mapOf("name" to "Foo", "coordinates" to coordinates.toDoubleArray(), "citizens" to citizens)

        await().atMost(30, TimeUnit.SECONDS).untilAsserted {
            val query = """
                |MATCH (p:Place)
                |RETURN p""".trimMargin()
            val result = db.beginTx().use {
                val result = it.execute(query).columnAs<Node>("p")
                if (result.hasNext()) {
                    result.next().allProperties
                } else {
                    emptyMap()
                }
            }

            assertThat(result).usingRecursiveComparison(withDoublePrecision()).isEqualTo(expected)
        }
    }

    @Test
    fun `the node pattern strategy must work also with AVRO data`() {
        //given
        val topic = UUID.randomUUID().toString()
        db.setConfig("kafka.key.deserializer", KafkaAvroDeserializer::class.java.name)
        db.setConfig("kafka.value.deserializer", KafkaAvroDeserializer::class.java.name)
        db.setConfig("kafka.schema.registry.url", KafkaEventSinkSuiteIT.schemaRegistry.getSchemaRegistryUrl())
        db.setConfig("streams.sink.topic.pattern.node.$topic", "(:Place{!name})")
        db.start()

        val PLACE_SCHEMA = SchemaBuilder.builder("com.namespace")
            .record("Place").fields()
            .name("name").type().stringType().noDefault()
            .name("coordinates").type().array().items().doubleType().noDefault()
            .name("citizens").type().longType().noDefault()
            .endRecord()
        val coordinates = listOf(42.30000, -11.22222)
        val citizens = 1_000_000L
        val struct = GenericRecordBuilder(PLACE_SCHEMA)
            .set("name", "Foo")
            .set("coordinates", coordinates)
            .set("citizens", citizens)
            .build()

        // when
        kafkaAvroProducer.send(ProducerRecord<GenericRecord, GenericRecord>(topic, null, struct)).get()

        // then
        val expected = mapOf("name" to "Foo", "coordinates" to coordinates.toDoubleArray(), "citizens" to citizens)

        await().atMost(30, TimeUnit.SECONDS).untilAsserted {
            val query = """
                |MATCH (p:Place)
                |RETURN p""".trimMargin()
            val result = db.beginTx().use {
                val result = it.execute(query).columnAs<Node>("p")
                if (result.hasNext()) {
                    result.next().allProperties
                } else {
                    emptyMap()
                }
            }

            assertThat(result).usingRecursiveComparison(withDoublePrecision()).isEqualTo(expected)
        }
    }

}