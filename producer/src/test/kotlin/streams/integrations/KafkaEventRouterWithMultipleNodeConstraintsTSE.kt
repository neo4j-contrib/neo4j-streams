package streams.integrations

import org.junit.Test
import streams.KafkaTestUtils
import streams.events.Constraint
import streams.events.NodePayload
import streams.events.OperationType
import streams.events.RelKeyStrategy
import streams.events.RelationshipPayload
import streams.events.StreamsConstraintType
import streams.extensions.execute
import streams.utils.JSONUtils
import streams.setConfig
import streams.start
import java.time.Duration
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class KafkaEventRouterWithMultipleNodeConstraintsTSE: KafkaEventRouterBaseTSE() {

    @Test
    fun testWithMultipleKeyStrategies() {
        val keyStrategyAll = "BOUGHT"
        val keyStrategyDefault = "ONE_PROP"
        val noKeyStrategy = "DEFAULT"

        val labelStart = "PersonConstr"
        val labelEnd = "ProductConstr"

        val personTopic = UUID.randomUUID().toString()
        val productTopic = UUID.randomUUID().toString()
        val topicWithStrategyAll = UUID.randomUUID().toString()
        val topicWithStrategyDefault = UUID.randomUUID().toString()
        val topicWithoutStrategy = UUID.randomUUID().toString()

        db.setConfig("streams.source.topic.nodes.$personTopic", "$labelStart{*}")
                .setConfig("streams.source.topic.nodes.$productTopic", "$labelEnd{*}")
                .setConfig("streams.source.topic.relationships.$topicWithStrategyAll", "$keyStrategyAll{*}")
                .setConfig("streams.source.topic.relationships.$topicWithStrategyDefault", "$keyStrategyDefault{*}")
                .setConfig("streams.source.topic.relationships.$topicWithoutStrategy", "$noKeyStrategy{*}")
                .setConfig("streams.source.topic.relationships.$topicWithStrategyAll.key_strategy", RelKeyStrategy.ALL.toString().toLowerCase())
                .setConfig("streams.source.topic.relationships.$topicWithStrategyDefault.key_strategy", RelKeyStrategy.DEFAULT.toString().toLowerCase())
                .setConfig("streams.source.schema.polling.interval", "0")
                .start()

        db.execute("CREATE CONSTRAINT ON (p:$labelStart) ASSERT p.surname IS UNIQUE")
        db.execute("CREATE CONSTRAINT ON (p:$labelStart) ASSERT p.name IS UNIQUE")
        db.execute("CREATE CONSTRAINT ON (p:$labelEnd) ASSERT p.name IS UNIQUE")

        val expectedSetConstraints = setOf(
                Constraint(labelStart, setOf("name"), StreamsConstraintType.UNIQUE),
                Constraint(labelStart, setOf("surname"), StreamsConstraintType.UNIQUE),
                Constraint(labelEnd, setOf("name"), StreamsConstraintType.UNIQUE)
        )
        val expectedPropsAllKeyStrategy = mapOf("name" to "Foo", "surname" to "Bar")
        val expectedPropsDefaultKeyStrategy = mapOf("name" to "Foo")
        val expectedEndProps = mapOf("name" to "One")

        KafkaTestUtils.createConsumer<String, ByteArray>(bootstrapServers = KafkaEventRouterSuiteIT.kafka.bootstrapServers)
            .use { consumer ->
                consumer.subscribe(listOf(personTopic))
                db.execute("CREATE (:$labelStart {name:'Foo', surname: 'Bar', address: 'Earth'})")
                val records = consumer.poll(Duration.ofSeconds(5))
                assertEquals(1, records.count())
                val record = records.first()
                assertNotNull(JSONUtils.readValue<Any>(record.key()))
                val value = JSONUtils.asStreamsTransactionEvent(record.value())
                val payload = value.payload as NodePayload
                val (properties, operation, schema) = Triple(payload.after!!.properties!!, value.meta.operation, value.schema)
                assertEquals(properties, mapOf("address" to "Earth", "name" to "Foo", "surname" to "Bar"))
                assertEquals(operation, OperationType.created)
                assertEquals(schema.properties, mapOf("address" to "String", "name" to "String", "surname" to "String"))
                val expectedSetConstraintsNode = setOf(Constraint(labelStart, setOf("name"), StreamsConstraintType.UNIQUE),
                        Constraint(labelStart, setOf("surname"), StreamsConstraintType.UNIQUE))
                assertEquals(expectedSetConstraintsNode, schema.constraints.toSet())
        }

        KafkaTestUtils.createConsumer<String, ByteArray>(bootstrapServers = KafkaEventRouterSuiteIT.kafka.bootstrapServers)
            .use { consumer ->
                consumer.subscribe(listOf(productTopic))
                db.execute("CREATE (:$labelEnd {name:'One', price: '100€'})")
                val records = consumer.poll(5000)
                assertEquals(1, records.count())
                val record = records.first()
                assertNotNull(JSONUtils.readValue<Any>(record.key()))
                val value = JSONUtils.asStreamsTransactionEvent(record.value())
                val payload = value.payload as NodePayload
                val (properties, operation, schema) = Triple(payload.after!!.properties!!, value.meta.operation, value.schema)
                assertEquals(properties, mapOf("name" to "One", "price" to "100€"))
                assertEquals(operation, OperationType.created)
                assertEquals(schema.properties, mapOf("name" to "String", "price" to "String"))
                assertEquals(setOf(Constraint(labelEnd, setOf("name"), StreamsConstraintType.UNIQUE)), schema.constraints.toSet())
        }

        // we test key_strategy=all with create/update/delete relationship
        KafkaTestUtils.createConsumer<String, ByteArray>(bootstrapServers = KafkaEventRouterSuiteIT.kafka.bootstrapServers)
            .use { consumer ->
                consumer.subscribe(listOf(topicWithStrategyAll))
                db.execute("""
                    |MATCH (p:$labelStart {name:'Foo'})
                    |MATCH (pp:$labelEnd {name:'One'})
                    |MERGE (p)-[:$keyStrategyAll]->(pp)
                """.trimMargin())
                val records = consumer.poll(5000)
                assertEquals(1, records.count())
                val record = records.first()
                assertNotNull(JSONUtils.readValue<Any>(record.key()))
                val value = JSONUtils.asStreamsTransactionEvent(record.value())
                val payload = value.payload as RelationshipPayload
                val (start, end, setConstraints) = Triple(payload.start, payload.end, value.schema.constraints.toSet())
                assertEquals(start.ids, expectedPropsAllKeyStrategy)
                assertEquals(end.ids, expectedEndProps)
                assertEquals(expectedSetConstraints, setConstraints)
                assertTrue(commonRelAssertions(value))

                db.execute("MATCH (p)-[rel:$keyStrategyAll]->(pp) SET rel.type = 'update'")
                val updatedRecords = consumer.poll(Duration.ofSeconds(5))
                assertEquals(1, updatedRecords.count())
                val updatedRecord = updatedRecords.first()
                assertNotNull(JSONUtils.readValue<Any>(updatedRecord.key()))
                val valueUpdate = JSONUtils.asStreamsTransactionEvent(updatedRecords.first().value())
                val payloadUpdate = valueUpdate.payload as RelationshipPayload
                val (startUpdate, endUpdate, setConstraintsUpdate) = Triple(payloadUpdate.start, payloadUpdate.end, valueUpdate.schema.constraints.toSet())
                assertEquals(startUpdate.ids, expectedPropsAllKeyStrategy)
                assertEquals(endUpdate.ids, expectedEndProps)
                assertEquals(expectedSetConstraints, setConstraintsUpdate)
                assertTrue(commonRelAssertionsUpdate(valueUpdate))

                db.execute("MATCH (p)-[rel:$keyStrategyAll]->(pp) DELETE rel")
                val deletedRecords = consumer.poll(Duration.ofSeconds(5))
                assertEquals(1, deletedRecords.count())
                val deletedRecord = deletedRecords.first()
                assertNotNull(JSONUtils.readValue<Any>(deletedRecord.key()))
                val valueDelete = JSONUtils.asStreamsTransactionEvent(deletedRecords.first().value())
                val payloadDelete = valueDelete.payload as RelationshipPayload
                val (startDelete, endDelete, setConstraintsDelete) = Triple(payloadDelete.start, payloadDelete.end, valueDelete.schema.constraints.toSet())
                assertEquals(startDelete.ids, expectedPropsAllKeyStrategy)
                assertEquals(endDelete.ids, expectedEndProps)
                assertEquals(expectedSetConstraints, setConstraintsDelete)
                assertTrue(commonRelAssertionsDelete(valueDelete))
        }

        // we test key_strategy=default with create/update/delete relationship
        KafkaTestUtils.createConsumer<String, ByteArray>(bootstrapServers = KafkaEventRouterSuiteIT.kafka.bootstrapServers)
                .use { consumer ->
            consumer.subscribe(listOf(topicWithStrategyDefault))
            db.execute("""
                |MATCH (p:$labelStart {name:'Foo'})
                |MATCH (pp:$labelEnd {name:'One'})
                |MERGE (p)-[:$keyStrategyDefault]->(pp)
            """.trimMargin())
            val records = consumer.poll(Duration.ofSeconds(5))
            assertEquals(1, records.count())
            val record = records.first()
            assertNotNull(JSONUtils.readValue<Any>(record.key()))
            val value = JSONUtils.asStreamsTransactionEvent(record.value())
            val payload = value.payload as RelationshipPayload
            val (start, end, setConstraints) = Triple(payload.start, payload.end, value.schema.constraints.toSet())
            assertEquals(start.ids, expectedPropsDefaultKeyStrategy)
            assertEquals(end.ids, expectedEndProps)
            assertEquals(expectedSetConstraints, setConstraints)
            assertTrue(commonRelAssertions(value))

            db.execute("MATCH (p)-[rel:$keyStrategyDefault]->(pp) SET rel.type = 'update'")
            val updatedRecords = consumer.poll(Duration.ofSeconds(5))
            assertEquals(1, updatedRecords.count())
            val updatedRecord = updatedRecords.first()
            assertNotNull(JSONUtils.readValue<Any>(updatedRecord.key()))
            val valueUpdate = JSONUtils.asStreamsTransactionEvent(updatedRecords.first().value())
            val payloadUpdate = valueUpdate.payload as RelationshipPayload
            val (startUpdate, endUpdate, setConstraintsUpdate) = Triple(payloadUpdate.start, payloadUpdate.end, valueUpdate.schema.constraints.toSet())
            assertEquals(startUpdate.ids, expectedPropsDefaultKeyStrategy)
            assertEquals(endUpdate.ids, expectedEndProps)
            assertEquals(expectedSetConstraints, setConstraintsUpdate)
            assertTrue(commonRelAssertionsUpdate(valueUpdate))

            db.execute("MATCH (p)-[rel:$keyStrategyDefault]->(pp) DELETE rel")
            val deletedRecords = consumer.poll(Duration.ofSeconds(5))
            assertEquals(1, deletedRecords.count())
            val deletedRecord = deletedRecords.first()
            assertNotNull(JSONUtils.readValue<Any>(deletedRecord.key()))
            val valueDelete = JSONUtils.asStreamsTransactionEvent(deletedRecords.first().value())
            val payloadDelete = valueDelete.payload as RelationshipPayload
            val (startDelete, endDelete, setConstraintsDelete) = Triple(payloadDelete.start, payloadDelete.end, valueDelete.schema.constraints.toSet())
            assertEquals(startDelete.ids, expectedPropsDefaultKeyStrategy)
            assertEquals(endDelete.ids, expectedEndProps)
            assertEquals(expectedSetConstraints, setConstraintsDelete)
            assertTrue(commonRelAssertionsDelete(valueDelete))
        }

        // we test a topic without key_strategy (that is, 'default') with create/update/delete relationship
        KafkaTestUtils.createConsumer<String, ByteArray>(bootstrapServers = KafkaEventRouterSuiteIT.kafka.bootstrapServers)
                .use { consumer ->
            consumer.subscribe(listOf(topicWithoutStrategy))
            db.execute("""
                |MATCH (p:$labelStart {name:'Foo'})
                |MATCH (pp:$labelEnd {name:'One'})
                |MERGE (p)-[:$noKeyStrategy]->(pp)
            """.trimMargin())
            val records = consumer.poll(5000)
            assertEquals(1, records.count())
            val record = records.first()
            assertNotNull(JSONUtils.readValue<Any>(record.key()))
            val value = JSONUtils.asStreamsTransactionEvent(record.value())
            val payload = value.payload as RelationshipPayload
            val (start, end, setConstraints) = Triple(payload.start, payload.end, value.schema.constraints.toSet())
            assertEquals(start.ids, expectedPropsDefaultKeyStrategy)
            assertEquals(end.ids, expectedEndProps)
            assertEquals(expectedSetConstraints, setConstraints)
            assertTrue(commonRelAssertions(value))

            db.execute("MATCH (p)-[rel:$noKeyStrategy]->(pp) SET rel.type = 'update'")
            val updatedRecords = consumer.poll(10000)
            assertEquals(1, updatedRecords.count())
            val updatedRecord = updatedRecords.first()
            assertNotNull(JSONUtils.readValue<Any>(updatedRecord.key()))
            val valueUpdate = JSONUtils.asStreamsTransactionEvent(updatedRecords.first().value())
            val payloadUpdate = valueUpdate.payload as RelationshipPayload
            val (startUpdate, endUpdate, setConstraintsUpdate) = Triple(payloadUpdate.start, payloadUpdate.end, valueUpdate.schema.constraints.toSet())
            assertEquals(startUpdate.ids, expectedPropsDefaultKeyStrategy)
            assertEquals(endUpdate.ids, expectedEndProps)
            assertEquals(expectedSetConstraints, setConstraintsUpdate)
            assertTrue(commonRelAssertionsUpdate(valueUpdate))

            db.execute("MATCH (p)-[rel:$noKeyStrategy]->(pp) DELETE rel")
            val deletedRecords = consumer.poll(10000)
            assertEquals(1, deletedRecords.count())
            val deletedRecord = deletedRecords.first()
            assertNotNull(JSONUtils.readValue<Any>(deletedRecord.key()))
            val valueDelete = JSONUtils.asStreamsTransactionEvent(deletedRecords.first().value())
            val payloadDelete = valueDelete.payload as RelationshipPayload
            val (startDelete, endDelete, setConstraintsDelete) = Triple(payloadDelete.start, payloadDelete.end, valueDelete.schema.constraints.toSet())
            assertEquals(startDelete.ids, expectedPropsDefaultKeyStrategy)
            assertEquals(endDelete.ids, expectedEndProps)
            assertEquals(expectedSetConstraints, setConstraintsDelete)
            assertTrue(commonRelAssertionsDelete(valueDelete))
        }
    }

}