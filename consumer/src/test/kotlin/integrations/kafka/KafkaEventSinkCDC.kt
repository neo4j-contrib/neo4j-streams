package integrations.kafka

import extension.newDatabase
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.producer.ProducerRecord
import org.hamcrest.Matchers
import org.junit.Test
import org.neo4j.function.ThrowingSupplier
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.test.assertion.Assert
import streams.events.Constraint
import streams.events.Meta
import streams.events.NodeChange
import streams.events.NodePayload
import streams.events.OperationType
import streams.events.RelationshipChange
import streams.events.RelationshipNodeChange
import streams.events.RelationshipPayload
import streams.events.Schema
import streams.events.StreamsConstraintType
import streams.events.StreamsTransactionEvent
import streams.serialization.JSONUtils
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals

class KafkaEventSinkCDC: KafkaEventSinkBase() {

    @Test
    fun shouldWriteDataFromSinkWithCDCTopic() {
        val topic = UUID.randomUUID().toString()
        graphDatabaseBuilder.setConfig("streams.sink.topic.cdc.sourceId", topic)
        graphDatabaseBuilder.setConfig("streams.sink.topic.cdc.sourceId.idName", "customIdN@me")
        graphDatabaseBuilder.setConfig("streams.sink.topic.cdc.sourceId.labelName", "CustomLabelN@me")
        db = graphDatabaseBuilder.newDatabase() as GraphDatabaseAPI

        val cdcDataStart = StreamsTransactionEvent(
                meta = Meta(timestamp = System.currentTimeMillis(),
                        username = "user",
                        txId = 1,
                        txEventId = 0,
                        txEventsCount = 3,
                        operation = OperationType.created
                ),
                payload = NodePayload(id = "0",
                        before = null,
                        after = NodeChange(properties = mapOf("name" to "Andrea", "comp@ny" to "LARUS-BA"), labels = listOf("User"))
                ),
                schema = Schema()
        )
        val cdcDataEnd = StreamsTransactionEvent(
                meta = Meta(timestamp = System.currentTimeMillis(),
                        username = "user",
                        txId = 1,
                        txEventId = 1,
                        txEventsCount = 3,
                        operation = OperationType.created
                ),
                payload = NodePayload(id = "1",
                        before = null,
                        after = NodeChange(properties = mapOf("name" to "Michael", "comp@ny" to "Neo4j"), labels = listOf("User Ext"))
                ),
                schema = Schema()
        )
        val cdcDataRelationship = StreamsTransactionEvent(
                meta = Meta(timestamp = System.currentTimeMillis(),
                        username = "user",
                        txId = 1,
                        txEventId = 2,
                        txEventsCount = 3,
                        operation = OperationType.created
                ),
                payload = RelationshipPayload(
                        id = "3",
                        start = RelationshipNodeChange(id = "0", labels = listOf("User"), ids = emptyMap()),
                        end = RelationshipNodeChange(id = "1", labels = listOf("User Ext"), ids = emptyMap()),
                        after = RelationshipChange(properties = mapOf("since" to 2014)),
                        before = null,
                        label = "KNOWS WHO"
                ),
                schema = Schema()
        )
        var producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(cdcDataStart))
        kafkaProducer.send(producerRecord).get()
        producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(cdcDataEnd))
        kafkaProducer.send(producerRecord).get()
        producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(cdcDataRelationship))
        kafkaProducer.send(producerRecord).get()

        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = """
                |MATCH p = (s:User:`CustomLabelN@me`{name:'Andrea', `comp@ny`:'LARUS-BA', `customIdN@me`: '0'})-[r:`KNOWS WHO`{since:2014, `customIdN@me`: '3'}]->(e:`User Ext`:`CustomLabelN@me`{name:'Michael', `comp@ny`:'Neo4j', `customIdN@me`: '1'})
                |RETURN count(p) AS count""".trimMargin()
            val result = db.execute(query).columnAs<Long>("count")
            result.hasNext() && result.next() == 1L && !result.hasNext()
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun shouldWriteDataFromSinkWithCDCSchemaTopic() {
        val topic = UUID.randomUUID().toString()
        graphDatabaseBuilder.setConfig("streams.sink.topic.cdc.schema", topic)
        db = graphDatabaseBuilder.newDatabase() as GraphDatabaseAPI

        val constraints = listOf(Constraint(label = "User", type = StreamsConstraintType.UNIQUE, properties = setOf("name", "surname")))
        val relSchema = Schema(properties = mapOf("since" to "Long"), constraints = constraints)
        val nodeSchema = Schema(properties = mapOf("name" to "String", "surname" to "String", "comp@ny" to "String"),
                constraints = constraints)
        val cdcDataStart = StreamsTransactionEvent(
                meta = Meta(timestamp = System.currentTimeMillis(),
                        username = "user",
                        txId = 1,
                        txEventId = 0,
                        txEventsCount = 3,
                        operation = OperationType.created
                ),
                payload = NodePayload(id = "0",
                        before = null,
                        after = NodeChange(properties = mapOf("name" to "Andrea", "surname" to "Santurbano", "comp@ny" to "LARUS-BA"), labels = listOf("User"))
                ),
                schema = nodeSchema
        )
        val cdcDataEnd = StreamsTransactionEvent(
                        meta = Meta(timestamp = System.currentTimeMillis(),
                        username = "user",
                        txId = 1,
                        txEventId = 1,
                        txEventsCount = 3,
                        operation = OperationType.created
                ),
                payload = NodePayload(id = "1",
                        before = null,
                        after = NodeChange(properties = mapOf("name" to "Michael", "surname" to "Hunger", "comp@ny" to "Neo4j"), labels = listOf("User"))
                ),
                schema = nodeSchema
        )
        val cdcDataRelationship = StreamsTransactionEvent(
                meta = Meta(timestamp = System.currentTimeMillis(),
                        username = "user",
                        txId = 1,
                        txEventId = 2,
                        txEventsCount = 3,
                        operation = OperationType.created
                ),
                payload = RelationshipPayload(
                        id = "2",
                        start = RelationshipNodeChange(id = "0", labels = listOf("User"), ids = mapOf("name" to "Andrea", "surname" to "Santurbano")),
                        end = RelationshipNodeChange(id = "1", labels = listOf("User"), ids = mapOf("name" to "Michael", "surname" to "Hunger")),
                        after = RelationshipChange(properties = mapOf("since" to 2014)),
                        before = null,
                        label = "KNOWS WHO"
                ),
                schema = relSchema
        )
        var producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(cdcDataStart))
        kafkaProducer.send(producerRecord).get()
        producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(cdcDataEnd))
        kafkaProducer.send(producerRecord).get()
        producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(cdcDataRelationship))
        kafkaProducer.send(producerRecord).get()

        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = """
                |MATCH p = (s:User{name:'Andrea', surname:'Santurbano', `comp@ny`:'LARUS-BA'})-[r:`KNOWS WHO`{since:2014}]->(e:User{name:'Michael', surname:'Hunger', `comp@ny`:'Neo4j'})
                |RETURN count(p) AS count
                |""".trimMargin()
            val result = db.execute(query).columnAs<Long>("count")
            result.hasNext() && result.next() == 1L && !result.hasNext()
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun writeDataFromSinkWithCDCSchemaTopicMultipleConstraintsAndLabels() = runBlocking {
        val topic = UUID.randomUUID().toString()
        graphDatabaseBuilder.setConfig("streams.sink.topic.cdc.schema", topic)
        db = graphDatabaseBuilder.newDatabase() as GraphDatabaseAPI

        val constraintsCharacter = listOf(
                Constraint(label = "Character", type = StreamsConstraintType.UNIQUE, properties = setOf("surname")),
                Constraint(label = "Character", type = StreamsConstraintType.UNIQUE, properties = setOf("name")),
                Constraint(label = "Character", type = StreamsConstraintType.UNIQUE, properties = setOf("country", "address"))
        )
        val constraintsWriter = listOf(
                Constraint(label = "Writer", type = StreamsConstraintType.UNIQUE, properties = setOf("lastName")),
                Constraint(label = "Writer", type = StreamsConstraintType.UNIQUE, properties = setOf("firstName"))
        )
        val relSchema = Schema(properties = mapOf("since" to "Long"), constraints = constraintsCharacter.plus(constraintsWriter))
        val nodeSchemaCharacter = Schema(properties = mapOf("name" to "String", "surname" to "String", "country" to "String", "address" to "String"), constraints = constraintsCharacter)
        val nodeSchemaWriter = Schema(properties = mapOf("firstName" to "String", "lastName" to "String"), constraints = constraintsWriter)
        val cdcDataStart = StreamsTransactionEvent(
                meta = Meta(timestamp = System.currentTimeMillis(),
                        username = "user",
                        txId = 1,
                        txEventId = 0,
                        txEventsCount = 3,
                        operation = OperationType.created
                ),
                payload = NodePayload(id = "0",
                        before = null,
                        after = NodeChange(properties = mapOf("name" to "Naruto", "surname" to "Uzumaki", "country" to "Japan", "address" to "Land of Leaf"), labels = listOf("Character"))
                ),
                schema = nodeSchemaCharacter
        )
        val cdcDataEnd = StreamsTransactionEvent(
                meta = Meta(timestamp = System.currentTimeMillis(),
                        username = "user",
                        txId = 1,
                        txEventId = 1,
                        txEventsCount = 3,
                        operation = OperationType.created
                ),
                payload = NodePayload(id = "1",
                        before = null,
                        after = NodeChange(properties = mapOf("firstName" to "Masashi", "lastName" to "Kishimoto", "address" to "Dunno"), labels = listOf("Writer"))
                ),
                schema = nodeSchemaWriter
        )
        val cdcDataRelationship = StreamsTransactionEvent(
                meta = Meta(timestamp = System.currentTimeMillis(),
                        username = "user",
                        txId = 1,
                        txEventId = 2,
                        txEventsCount = 3,
                        operation = OperationType.created
                ),
                payload = RelationshipPayload(
                        id = "2",
                        // leverage on first label alphabetically and, with the same name, the first ids alphabetically, that is name, so we take the 2 previously created nodes
                        start = RelationshipNodeChange(id = "0", labels = listOf("Character"),
                                ids = mapOf("name" to "Naruto", "surname" to "Osvaldo", "address" to "Land of Sand")),
                        end = RelationshipNodeChange(id = "1", labels = listOf("Writer"),
                                ids = mapOf("firstName" to "Masashi", "lastName" to "Franco")),
                        after = RelationshipChange(properties = mapOf("since" to 1999)),
                        before = null,
                        label = "HAS WRITTEN"
                ),
                schema = relSchema
        )
        var producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(cdcDataStart))
        kafkaProducer.send(producerRecord).get()
        producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(cdcDataEnd))
        kafkaProducer.send(producerRecord).get()
        producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(cdcDataRelationship))
        kafkaProducer.send(producerRecord).get()

        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = """
                |MATCH p = (s:Character)-[r:`HAS WRITTEN`{since: 1999}]->(e:Writer)
                |RETURN count(p) AS count
                |""".trimMargin()
            db.execute(query).use {
                val result = it.columnAs<Long>("count")
                result.hasNext() && result.next() == 1L && !result.hasNext()
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)

        val cypherCountNodes = "MATCH (n) RETURN count(n) AS count"
        var countNodes = db.execute(cypherCountNodes).columnAs<Long>("count").next()
        assertEquals(2L, countNodes)

        // another CDC data, not matching the previously created nodes
        val cdcDataRelationshipNotMatched = StreamsTransactionEvent(
                meta = Meta(timestamp = System.currentTimeMillis(),
                        username = "user",
                        txId = 1,
                        txEventId = 2,
                        txEventsCount = 3,
                        operation = OperationType.created
                ),
                payload = RelationshipPayload(
                        id = "2",
                        // leverage on first ids alphabetically, that is name, so create 2 additional nodes
                        start = RelationshipNodeChange(id = "1", labels = listOf("Character"), ids = mapOf("name" to "Invalid", "surname" to "Uzumaki")),
                        end = RelationshipNodeChange(id = "2", labels = listOf("Writer"), ids = mapOf("firstName" to "AnotherInvalid", "surname" to "Namikaze")),
                        after = RelationshipChange(properties = mapOf("since" to 2000)),
                        before = null,
                        label = "HAS WRITTEN"
                ),
                schema = relSchema
        )

        producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(cdcDataRelationshipNotMatched))
        kafkaProducer.send(producerRecord).get()

        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = """
                |MATCH p = (s:Character)-[r:`HAS WRITTEN`{since:2000}]->(e:Writer)
                |RETURN count(p) AS count
                |""".trimMargin()
            db.execute(query).use {
                val result = it.columnAs<Long>("count")
                result.hasNext() && result.next() == 1L && !result.hasNext()
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)

        // create another node
        countNodes = db.execute(cypherCountNodes).use { it.columnAs<Long>("count").next() }
        assertEquals(4L, countNodes)
    }

    @Test
    fun shouldWriteDataFromSinkWithCDCSchemaTopicWithMultipleConstraints() = runBlocking {
        val topic = UUID.randomUUID().toString()
        graphDatabaseBuilder.setConfig("streams.sink.topic.cdc.schema", topic)
        db = graphDatabaseBuilder.newDatabase() as GraphDatabaseAPI

        val constraints = listOf(
                Constraint(label = "User", type = StreamsConstraintType.UNIQUE, properties = setOf("name")),
                Constraint(label = "User", type = StreamsConstraintType.UNIQUE, properties = setOf("country", "address")),
                Constraint(label = "User", type = StreamsConstraintType.UNIQUE, properties = setOf("surname"))
        )
        val relSchema = Schema(properties = mapOf("since" to "Long"), constraints = constraints)
        val nodeSchema = Schema(properties = mapOf("name" to "String", "surname" to "String", "country" to "String", "address" to "String"), constraints = constraints)
        val cdcDataStart = StreamsTransactionEvent(
                meta = Meta(timestamp = System.currentTimeMillis(),
                        username = "user",
                        txId = 1,
                        txEventId = 0,
                        txEventsCount = 3,
                        operation = OperationType.created
                ),
                payload = NodePayload(id = "0",
                        before = null,
                        after = NodeChange(properties = mapOf("name" to "Naruto", "surname" to "Uzumaki", "country" to "Japan", "address" to "Land of Leaf"), labels = listOf("User"))
                ),
                schema = nodeSchema
        )
        val cdcDataEnd = StreamsTransactionEvent(
                meta = Meta(timestamp = System.currentTimeMillis(),
                        username = "user",
                        txId = 1,
                        txEventId = 1,
                        txEventsCount = 3,
                        operation = OperationType.created
                ),
                payload = NodePayload(id = "1",
                        before = null,
                        after = NodeChange(properties = mapOf("name" to "Minato", "surname" to "Namikaze", "country" to "Japan", "address" to "Land of Leaf"), labels = listOf("User"))
                ),
                schema = nodeSchema
        )
        val cdcDataRelationship = StreamsTransactionEvent(
                meta = Meta(timestamp = System.currentTimeMillis(),
                        username = "user",
                        txId = 1,
                        txEventId = 2,
                        txEventsCount = 3,
                        operation = OperationType.created
                ),
                payload = RelationshipPayload(
                        id = "2",
                        // leverage on first ids alphabetically, that is name, so we take the 2 previously created nodes
                        start = RelationshipNodeChange(id = "99", labels = listOf("User"), ids = mapOf("name" to "Naruto", "surname" to "Osvaldo", "address" to "Land of Sand")),
                        end = RelationshipNodeChange(id = "88", labels = listOf("User"), ids = mapOf("name" to "Minato", "surname" to "Franco", "address" to "Land of Fire")),
                        after = RelationshipChange(properties = mapOf("since" to 2014)),
                        before = null,
                        label = "KNOWS WHO"
                ),
                schema = relSchema
        )
        var producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(cdcDataStart))
        kafkaProducer.send(producerRecord).get()
        producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(cdcDataEnd))
        kafkaProducer.send(producerRecord).get()
        producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(cdcDataRelationship))
        kafkaProducer.send(producerRecord).get()

        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = """
                |MATCH p = (s:User)-[r:`KNOWS WHO` {since: 2014}]->(e:User)
                |RETURN count(p) AS count
                |""".trimMargin()
            db.execute(query).use {
                val result = it.columnAs<Long>("count")
                result.hasNext() && result.next() == 1L && !result.hasNext()
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)

        val cypherCountNodes = "MATCH (n) RETURN count(n) AS count"
        var countNodes = db.execute(cypherCountNodes).columnAs<Long>("count").next()
        assertEquals(2L, countNodes)

        // another CDC data, not matching the previously created nodes
        val cdcDataRelationshipNotMatched = StreamsTransactionEvent(
                meta = Meta(timestamp = System.currentTimeMillis(),
                        username = "user",
                        txId = 1,
                        txEventId = 2,
                        txEventsCount = 3,
                        operation = OperationType.created
                ),
                payload = RelationshipPayload(
                        id = "2",
                        // leverage on first ids alphabetically, that is name, so create 2 additional nodes
                        start = RelationshipNodeChange(id = "1", labels = listOf("User"), ids = mapOf("name" to "Invalid", "surname" to "Uzumaki")),
                        end = RelationshipNodeChange(id = "2", labels = listOf("User"), ids = mapOf("name" to "AnotherInvalid", "surname" to "Namikaze")),
                        after = RelationshipChange(properties = mapOf("since" to 2000)),
                        before = null,
                        label = "KNOWS ANOTHER"
                ),
                schema = relSchema
        )

        producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(cdcDataRelationshipNotMatched))
        kafkaProducer.send(producerRecord).get()

        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = """
                |MATCH p = (s:User)-[r:`KNOWS ANOTHER` {since:2000}]->(e:User)
                |RETURN count(p) AS count
                |""".trimMargin()
            db.execute(query).use {
                val result = it.columnAs<Long>("count")
                result.hasNext() && result.next() == 1L && !result.hasNext()
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)

        // create another node
        countNodes = db.execute(cypherCountNodes).columnAs<Long>("count").next()
        assertEquals(4L, countNodes)
    }

    @Test
    fun shouldDeleteDataFromSinkWithCDCSchemaTopic() {
        val topic = UUID.randomUUID().toString()
        graphDatabaseBuilder.setConfig("streams.sink.topic.cdc.schema", topic)
        db = graphDatabaseBuilder.newDatabase() as GraphDatabaseAPI

        db.execute("CREATE (s:User{name:'Andrea', surname:'Santurbano', `comp@ny`:'LARUS-BA'})-[r:`KNOWS WHO`{since:2014}]->(e:User{name:'Michael', surname:'Hunger', `comp@ny`:'Neo4j'})").close()
        val nodeSchema = Schema(properties = mapOf("name" to "String", "surname" to "String", "comp@ny" to "String"),
                constraints = listOf(Constraint(label = "User", type =  StreamsConstraintType.UNIQUE, properties = setOf("name", "surname"))))
        val cdcDataStart = StreamsTransactionEvent(
                meta = Meta(timestamp = System.currentTimeMillis(),
                        username = "user",
                        txId = 1,
                        txEventId = 0,
                        txEventsCount = 3,
                        operation = OperationType.deleted
                ),
                payload = NodePayload(id = "0",
                        after = null,
                        before = NodeChange(properties = mapOf("name" to "Andrea", "surname" to "Santurbano", "comp@ny" to "LARUS-BA"), labels = listOf("User"))
                ),
                schema = nodeSchema
        )

        val producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(cdcDataStart))
        kafkaProducer.send(producerRecord).get()

        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = """
                |MATCH p = (s:User{name:'Andrea', surname:'Santurbano', `comp@ny`:'LARUS-BA'})-[r:`KNOWS WHO`{since:2014}]->(e:User{name:'Michael', surname:'Hunger', `comp@ny`:'Neo4j'})
                |RETURN count(p) AS count
                |""".trimMargin()
            val result = db.execute(query).columnAs<Long>("count")
            result.hasNext() && result.next() == 0L && !result.hasNext()
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }
}