package integrations.kafka

import kotlinx.coroutines.runBlocking
import extension.newDatabase
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.hamcrest.Matchers
import org.junit.Test
import org.neo4j.function.ThrowingSupplier
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.test.assertion.Assert
import streams.serialization.JSONUtils
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals

class KafkaEventSinkPattern : KafkaEventSinkBase() {
    @Test
    fun shouldWorkWithNodePatternTopic() = runBlocking {
        val topic = UUID.randomUUID().toString()
        graphDatabaseBuilder.setConfig("streams.sink.topic.pattern.node.$topic",
                "(:User{!userId,name,surname,address.city})")
        db = graphDatabaseBuilder.newDatabase() as GraphDatabaseAPI

        val data = mapOf("userId" to 1, "name" to "Andrea", "surname" to "Santurbano",
                "address" to mapOf("city" to "Venice", "CAP" to "30100"))

        var producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(data))
        kafkaProducer.send(producerRecord).get()
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = "MATCH (n:User{name: 'Andrea', surname: 'Santurbano', userId: 1, `address.city`: 'Venice'}) RETURN count(n) AS count"
            val result = db.execute(query).columnAs<Long>("count")
            result.hasNext() && result.next() == 1L && !result.hasNext()
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun shouldWorkWithRelPatternTopic() = runBlocking {
        val topic = UUID.randomUUID().toString()
        graphDatabaseBuilder.setConfig("streams.sink.topic.pattern.relationship.$topic",
                "(:User{!sourceId,sourceName,sourceSurname})-[:KNOWS]->(:User{!targetId,targetName,targetSurname})")
        db = graphDatabaseBuilder.newDatabase() as GraphDatabaseAPI
        val data = mapOf("sourceId" to 1, "sourceName" to "Andrea", "sourceSurname" to "Santurbano",
                "targetId" to 1, "targetName" to "Michael", "targetSurname" to "Hunger", "since" to 2014)

        var producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(data))
        kafkaProducer.send(producerRecord).get()
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = """
                MATCH p = (s:User{sourceName: 'Andrea', sourceSurname: 'Santurbano', sourceId: 1})-[:KNOWS{since: 2014}]->(e:User{targetName: 'Michael', targetSurname: 'Hunger', targetId: 1})
                RETURN count(p) AS count
            """.trimIndent()
            val result = db.execute(query).columnAs<Long>("count")
            result.hasNext() && result.next() == 1L && !result.hasNext()
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun `should mange the Tombstone record for the Node Pattern Strategy`() = runBlocking {
        val topic = UUID.randomUUID().toString()
        graphDatabaseBuilder.setConfig("streams.sink.topic.pattern.node.$topic",
                "(:User{!userId,name,surname})")
        db = graphDatabaseBuilder.newDatabase() as GraphDatabaseAPI

        db.execute("CREATE (u:User{userId: 1, name: 'Andrea', surname: 'Santurbano'})").close()
        val count = db.execute("MATCH (n:User) RETURN count(n) AS count").columnAs<Long>("count").next()
        assertEquals(1L, count)


        val kafkaProperties = Properties()
        kafkaProperties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaEventSinkSuiteIT.kafka.bootstrapServers
        kafkaProperties["group.id"] = "neo4j"
        kafkaProperties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = ByteArraySerializer::class.java
        kafkaProperties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = ByteArraySerializer::class.java

        val kafkaProducer: KafkaProducer<ByteArray, ByteArray> = KafkaProducer(kafkaProperties)

        val data = mapOf("userId" to 1, "name" to "Andrea", "surname" to "Santurbano")

        val producerRecord = ProducerRecord<ByteArray, ByteArray>(topic,  JSONUtils.writeValueAsBytes(data), null)
        kafkaProducer.send(producerRecord).get()
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = "MATCH (n:User) RETURN count(n) AS count"
            val result = db.execute(query).columnAs<Long>("count")
            result.hasNext() && result.next() == 0L && !result.hasNext()
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }
}