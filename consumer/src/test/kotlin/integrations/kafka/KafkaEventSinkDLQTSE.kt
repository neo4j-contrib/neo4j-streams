package integrations.kafka

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.hamcrest.Matchers
import org.junit.Test
import org.neo4j.function.ThrowingSupplier
import streams.Assert
import streams.KafkaTestUtils
import streams.extensions.execute
import streams.utils.JSONUtils
import streams.service.errors.ErrorService
import streams.setConfig
import streams.start
import java.util.UUID
import java.util.concurrent.TimeUnit

class KafkaEventSinkDLQTSE : KafkaEventSinkBaseTSE() {
    @Test
    fun `should send data to the DLQ because of QueryExecutionException`() {
        val topic = UUID.randomUUID().toString()
        val dlqTopic = UUID.randomUUID().toString()
        db.setConfig("streams.sink.topic.cypher.$topic","MERGE (c:Customer {id: event.id})")
        db.setConfig("streams.sink."+ErrorService.ErrorConfig.TOLERANCE, "all")
        db.setConfig("streams.sink."+ErrorService.ErrorConfig.DLQ_TOPIC, dlqTopic)
        db.setConfig("streams.sink."+ErrorService.ErrorConfig.DLQ_HEADERS, "true")
        db.setConfig("streams.sink."+ErrorService.ErrorConfig.DLQ_HEADER_PREFIX, "__streams.errors.")
        db.start()
        val data = mapOf("id" to null, "name" to "Andrea", "surname" to "Santurbano")

        var producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(data))
        kafkaProducer.send(producerRecord).get()
        val dlqConsumer = KafkaTestUtils.createConsumer<ByteArray, ByteArray>(
                bootstrapServers = KafkaEventSinkSuiteIT.kafka.bootstrapServers,
                schemaRegistryUrl = KafkaEventSinkSuiteIT.schemaRegistry.getSchemaRegistryUrl(),
                keyDeserializer = ByteArrayDeserializer::class.java.name,
                valueDeserializer = ByteArrayDeserializer::class.java.name,
                topics = *arrayOf(dlqTopic))

        dlqConsumer.use {
            Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
                val query = """
                MATCH (c:Customer)
                RETURN count(c) AS count
            """.trimIndent()

                val records = dlqConsumer.poll(5000)
                val record = if (records.isEmpty) null else records.records(dlqTopic).iterator().next()
                val headers = record?.headers()?.map { it.key() to String(it.value()) }?.toMap().orEmpty()
                val value = if (record != null) JSONUtils.readValue<Any>(record.value()!!) else emptyMap<String, Any>()
                db.execute(query) {
                    val result = it.columnAs<Long>("count")
                    !records.isEmpty && headers.size == 7 && value == data && result.hasNext() && result.next() == 0L && !result.hasNext()
                            && headers["__streams.errors.exception.class.name"] == "org.neo4j.graphdb.QueryExecutionException"
                }
            }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
        }
    }

    @Test
    fun `should send data to the DLQ because of JsonParseException`() {
        val topic = UUID.randomUUID().toString()
        val dlqTopic = UUID.randomUUID().toString()
        db.setConfig("streams.sink.topic.cypher.$topic","MERGE (c:Customer {id: event.id})")
        db.setConfig("streams.sink."+ErrorService.ErrorConfig.TOLERANCE, "all")
        db.setConfig("streams.sink."+ErrorService.ErrorConfig.DLQ_TOPIC, dlqTopic)
        db.setConfig("streams.sink."+ErrorService.ErrorConfig.DLQ_HEADERS, "true")
        db.setConfig("streams.sink."+ErrorService.ErrorConfig.DLQ_HEADER_PREFIX, "__streams.errors.")
        db.start()

        val data = """{id: 1, "name": "Andrea", "surname": "Santurbano"}"""

        var producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(),
                data.toByteArray())
        kafkaProducer.send(producerRecord).get()
        val dlqConsumer = KafkaTestUtils.createConsumer<ByteArray, ByteArray>(
                bootstrapServers = KafkaEventSinkSuiteIT.kafka.bootstrapServers,
                schemaRegistryUrl = KafkaEventSinkSuiteIT.schemaRegistry.getSchemaRegistryUrl(),
                keyDeserializer = ByteArrayDeserializer::class.java.name,
                valueDeserializer = ByteArrayDeserializer::class.java.name,
                topics = *arrayOf(dlqTopic))
        dlqConsumer.use {
            Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
                val query = """
                MATCH (c:Customer)
                RETURN count(c) AS count
            """.trimIndent()
                db.execute(query) { result ->
                    val count = result.columnAs<Long>("count")
                    val records = dlqConsumer.poll(5000)
                    val record = if (records.isEmpty) null else records.records(dlqTopic).iterator().next()
                    val headers = record?.headers()?.map { it.key() to String(it.value()) }?.toMap().orEmpty()
                    val value = if (record != null) String(record.value()) else emptyMap<String, Any>()
                    !records.isEmpty && headers.size == 7 && data == value && count.hasNext() && count.next() == 0L && !count.hasNext()
                            && headers["__streams.errors.exception.class.name"] == "com.fasterxml.jackson.core.JsonParseException"
                }
            }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
        }
    }

}