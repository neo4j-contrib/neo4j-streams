package integrations.kafka

import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.hamcrest.Matchers
import org.junit.Ignore
import org.junit.Test
import org.neo4j.function.ThrowingSupplier
import streams.Assert
import streams.extensions.execute
import streams.utils.JSONUtils
import streams.setConfig
import streams.start
import java.util.Properties
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals

class KafkaEventSinkPatternTSE : KafkaEventSinkBaseTSE() {
    @Test
    fun shouldWorkWithNodePatternTopic() = runBlocking {
        val topic = UUID.randomUUID().toString()
        db.setConfig("streams.sink.topic.pattern.node.$topic",
                "(:User{!userId,name,surname,address.city})")
        db.start()

        val data = mapOf("userId" to 1, "name" to "Andrea", "surname" to "Santurbano",
                "address" to mapOf("city" to "Venice", "CAP" to "30100"))

        var producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(data))
        kafkaProducer.send(producerRecord).get()
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = "MATCH (n:User{name: 'Andrea', surname: 'Santurbano', userId: 1, `address.city`: 'Venice'}) RETURN count(n) AS count"
            db.execute(query) {
                val result = it.columnAs<Long>("count")
                result.hasNext() && result.next() == 1L && !result.hasNext()
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    @Ignore("fix it")
    fun shouldInsertNodesAndRelationships() = runBlocking {
        val user = UUID.randomUUID().toString()
        val product = UUID.randomUUID().toString()
        val rel = UUID.randomUUID().toString()
        db.setConfig("streams.sink.topic.pattern.node.$user",
                "(:User{!userId,name,surname,address.city})")
        db.setConfig("streams.sink.topic.pattern.node.$product",
                "(:Product{!productId})")
        db.setConfig("streams.sink.topic.pattern.relationship.$rel",
                "(:User{!userId})-[:BOUGHT]->(:Product{!productId})")
        db.start()
        db.execute("CREATE CONSTRAINT neo4j_streams_user_constraint ON (u:User) ASSERT (u.userId) IS UNIQUE")
        db.execute("CREATE CONSTRAINT neo4j_streams_product_constraint ON (p:Product) ASSERT (p.productId) IS UNIQUE")
        val userData = mapOf("userId" to 1, "name" to "Andrea Santurbano")
        val productData = mapOf("productId" to 10, "name" to "My Product")
        val relData = mapOf("productId" to 10, "userId" to 1, "quantity" to 100)
        var producerRecord = ProducerRecord(user, UUID.randomUUID().toString(),
                JSONUtils.writeValueAsBytes(userData))
        kafkaProducer.send(producerRecord).get()
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = """
                MATCH (u:User{userId: 1, name: 'Andrea Santurbano'})
                RETURN count(u) AS count
            """.trimIndent()
            db.execute(query) {
                val result = it.columnAs<Long>("count")
                result.hasNext() && result.next() == 1L && !result.hasNext()
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
        producerRecord = ProducerRecord(product, UUID.randomUUID().toString(),
                JSONUtils.writeValueAsBytes(productData))
        kafkaProducer.send(producerRecord).get()
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = """
                MATCH (p:Product{productId: 10, name: 'My Product'})
                RETURN count(p) AS count
            """.trimIndent()
            db.execute(query) {
                val result = it.columnAs<Long>("count")
                result.hasNext() && result.next() == 1L && !result.hasNext()
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
        producerRecord = ProducerRecord(rel, UUID.randomUUID().toString(),
                JSONUtils.writeValueAsBytes(relData))
        kafkaProducer.send(producerRecord).get()
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = """
                MATCH p = (s:User{userId: 1, name: 'Andrea Santurbano'})-[:BOUGHT{quantity: 100}]->(e:Product{productId: 10, name: 'My Product'})
                RETURN count(p) AS count
            """.trimIndent()
            db.execute(query) {
                val result = it.columnAs<Long>("count")
                result.hasNext() && result.next() == 1L && !result.hasNext()
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }


    @Test
    fun shouldWorkWithRelPatternTopic() = runBlocking {
        val topic = UUID.randomUUID().toString()
        db.setConfig("streams.sink.topic.pattern.relationship.$topic",
                "(:User{!sourceId,sourceName,sourceSurname})-[:KNOWS]->(:User{!targetId,targetName,targetSurname})")
        db.start()
        val data = mapOf("sourceId" to 1, "sourceName" to "Andrea", "sourceSurname" to "Santurbano",
                "targetId" to 1, "targetName" to "Michael", "targetSurname" to "Hunger", "since" to 2014)

        var producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(data))
        kafkaProducer.send(producerRecord).get()
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = """
                MATCH p = (s:User{sourceName: 'Andrea', sourceSurname: 'Santurbano', sourceId: 1})-[:KNOWS{since: 2014}]->(e:User{targetName: 'Michael', targetSurname: 'Hunger', targetId: 1})
                RETURN count(p) AS count
            """.trimIndent()
            db.execute(query) {
                val result = it.columnAs<Long>("count")
                result.hasNext() && result.next() == 1L && !result.hasNext()
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun `should mange the Tombstone record for the Node Pattern Strategy`() = runBlocking {
        val topic = UUID.randomUUID().toString()
        db.setConfig("streams.sink.topic.pattern.node.$topic",
                "(:User{!userId,name,surname})")
        db.start()

        db.execute("CREATE (u:User{userId: 1, name: 'Andrea', surname: 'Santurbano'})")
        val count = db.execute("MATCH (n:User) RETURN count(n) AS count") { it.columnAs<Long>("count").next() }
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
            db.execute(query) {
                val result = it.columnAs<Long>("count")
                result.hasNext() && result.next() == 0L && !result.hasNext()
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }
}