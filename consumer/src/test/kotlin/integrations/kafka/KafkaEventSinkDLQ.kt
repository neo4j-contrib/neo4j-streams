package integrations.kafka

import integrations.kafka.KafkaTestUtils.createConsumer
import extension.newDatabase
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.hamcrest.Matchers
import org.junit.Test
import org.neo4j.function.ThrowingSupplier
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.test.assertion.Assert
import streams.serialization.JSONUtils
import streams.service.errors.ErrorService
import java.util.*
import java.util.concurrent.TimeUnit

class KafkaEventSinkDLQ : KafkaEventSinkBase() {
    @Test
    fun `should send data to the DLQ because of QueryExecutionException`() {
        val topic = UUID.randomUUID().toString()
        val dlqTopic = UUID.randomUUID().toString()
        graphDatabaseBuilder.setConfig("streams.sink.topic.cypher.$topic","MERGE (c:Customer {id: event.id})")
        graphDatabaseBuilder.setConfig("streams.sink."+ErrorService.ErrorConfig.TOLERANCE, "all")
        graphDatabaseBuilder.setConfig("streams.sink."+ErrorService.ErrorConfig.DLQ_TOPIC, dlqTopic)
        graphDatabaseBuilder.setConfig("streams.sink."+ErrorService.ErrorConfig.DLQ_HEADERS, "true")
        graphDatabaseBuilder.setConfig("streams.sink."+ErrorService.ErrorConfig.DLQ_HEADER_PREFIX, "__streams.errors.")
        db = graphDatabaseBuilder.newDatabase() as GraphDatabaseAPI
        val data = mapOf("id" to null, "name" to "Andrea", "surname" to "Santurbano")

        var producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(data))
        kafkaProducer.send(producerRecord).get()
        val dlqConsumer = createConsumer<ByteArray, ByteArray>(
                kafka = KafkaEventSinkSuiteIT.kafka,
                schemaRegistry = KafkaEventSinkSuiteIT.schemaRegistry,
                keyDeserializer = ByteArrayDeserializer::class.java.name,
                valueDeserializer = ByteArrayDeserializer::class.java.name,
                topics = *arrayOf(dlqTopic))

        dlqConsumer.let {
            Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
                val query = """
                MATCH (c:Customer)
                RETURN count(c) AS count
            """.trimIndent()
                val result = db.execute(query).columnAs<Long>("count")

                val records = dlqConsumer.poll(5000)
                val record = if (records.isEmpty) null else records.records(dlqTopic).iterator().next()
                val headers = record?.headers()?.map { it.key() to String(it.value()) }?.toMap().orEmpty()
                val value = if (record != null) JSONUtils.readValue<Any>(record.value()!!) else emptyMap<String, Any>()
                !records.isEmpty && headers.size == 7 && value == data && result.hasNext() && result.next() == 0L && !result.hasNext()
                        && headers["__streams.errors.exception.class.name"] == "org.neo4j.graphdb.QueryExecutionException"
            }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
            it.close()
        }
    }

    @Test
    fun `should send data to the DLQ because of JsonParseException`() {
        val topic = UUID.randomUUID().toString()
        val dlqTopic = UUID.randomUUID().toString()
        graphDatabaseBuilder.setConfig("streams.sink.topic.cypher.$topic","MERGE (c:Customer {id: event.id})")
        graphDatabaseBuilder.setConfig("streams.sink."+ErrorService.ErrorConfig.TOLERANCE, "all")
        graphDatabaseBuilder.setConfig("streams.sink."+ErrorService.ErrorConfig.DLQ_TOPIC, dlqTopic)
        graphDatabaseBuilder.setConfig("streams.sink."+ErrorService.ErrorConfig.DLQ_HEADERS, "true")
        graphDatabaseBuilder.setConfig("streams.sink."+ErrorService.ErrorConfig.DLQ_HEADER_PREFIX, "__streams.errors.")
        db = graphDatabaseBuilder.newDatabase() as GraphDatabaseAPI

        val data = """{id: 1, "name": "Andrea", "surname": "Santurbano"}"""

        var producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(),
                data.toByteArray())
        kafkaProducer.send(producerRecord).get()
        val dlqConsumer = createConsumer<ByteArray, ByteArray>(
                kafka = KafkaEventSinkSuiteIT.kafka,
                schemaRegistry = KafkaEventSinkSuiteIT.schemaRegistry,
                keyDeserializer = ByteArrayDeserializer::class.java.name,
                valueDeserializer = ByteArrayDeserializer::class.java.name,
                topics = *arrayOf(dlqTopic))
        dlqConsumer.let {
            Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
                val query = """
                MATCH (c:Customer)
                RETURN count(c) AS count
            """.trimIndent()
                val result = db.execute(query).columnAs<Long>("count")


                val records = dlqConsumer.poll(5000)
                val record = if (records.isEmpty) null else records.records(dlqTopic).iterator().next()
                val headers = record?.headers()?.map { it.key() to String(it.value()) }?.toMap().orEmpty()
                val value = if (record != null) String(record.value()) else emptyMap<String, Any>()
                !records.isEmpty && headers.size == 7 && data == value && result.hasNext() && result.next() == 0L && !result.hasNext()
                        && headers["__streams.errors.exception.class.name"] == "com.fasterxml.jackson.core.JsonParseException"
            }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
            it.close()
        }
    }

}