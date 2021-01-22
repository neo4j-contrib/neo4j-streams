package streams.integrations

import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.hamcrest.Matchers
import org.junit.Test
import org.neo4j.function.ThrowingSupplier
import streams.Assert
import streams.events.EntityType
import streams.events.NodeChange
import streams.events.NodePayload
import streams.events.OperationType
import streams.events.RelationshipPayload
import streams.extensions.execute
import streams.kafka.KafkaConfiguration
import streams.KafkaTestUtils
import streams.utils.JSONUtils
import streams.setConfig
import streams.start
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class KafkaEventRouterSimpleTSE: KafkaEventRouterBaseTSE() {

    @Test
    fun testCreateNode() {
        val topic = UUID.randomUUID().toString()
        db.setConfig("streams.source.topic.nodes.$topic", "Person{*}").start()
        kafkaConsumer.subscribe(listOf(topic))
        db.execute("CREATE (:Person {name:'John Doe', age:42})")
        val records = kafkaConsumer.poll(5000)
        assertEquals(1, records.count())
        assertEquals(true, records.all {
            JSONUtils.asStreamsTransactionEvent(it.value()).let {
                val after = it.payload.after as NodeChange
                val labels = after.labels
                val propertiesAfter = after.properties
                labels == listOf("Person") && propertiesAfter == mapOf("name" to "John Doe", "age" to 42)
                        && it.meta.operation == OperationType.created
                        && it.schema.properties == mapOf("name" to "String", "age" to "Long")
                        && it.schema.constraints.isEmpty()
            }
        })
    }

    @Test
    fun testCreateRelationshipWithRelRouting() {
        db.setConfig("streams.source.topic.relationships.knows", "KNOWS{*}").start()
        kafkaConsumer.subscribe(listOf("knows"))
        db.execute("CREATE (:Person {name:'Andrea'})-[:KNOWS{since: 2014}]->(:Person {name:'Michael'})")
        val records = kafkaConsumer.poll(5000)
        assertEquals(1, records.count())
        assertEquals(true, records.all {
            JSONUtils.asStreamsTransactionEvent(it.value()).let {
                var payload = it.payload as RelationshipPayload
                val properties = payload.after!!.properties!!
                payload.type == EntityType.relationship && payload.label == "KNOWS"
                        && properties["since"] == 2014
                        && it.schema.properties == mapOf("since" to "Long")
                        && it.schema.constraints.isEmpty()
            }
        })
    }

    @Test
    fun testCreateNodeWithNodeRouting() {
        db.setConfig("streams.source.topic.nodes.person", "Person{*}").start()
        kafkaConsumer.subscribe(listOf("person"))
        db.execute("CREATE (:Person {name:'Andrea'})")
        val records = kafkaConsumer.poll(5000)
        assertEquals(1, records.count())
        assertEquals(true, records.all {
            JSONUtils.asStreamsTransactionEvent(it.value()).let {
                val payload = it.payload as NodePayload
                val labels = payload.after!!.labels!!
                val properties = payload.after!!.properties
                labels == listOf("Person") && properties == mapOf("name" to "Andrea")
                        && it.meta.operation == OperationType.created
                        && it.schema.properties == mapOf("name" to "String")
                        && it.schema.constraints.isEmpty()
            }
        })
    }

    @Test
    fun testMultiTopicPatternConfig() = runBlocking {
        val getRecordCount = {config: KafkaConfiguration, topic: String ->
            val kafkaConsumer = KafkaTestUtils.createConsumer<String, ByteArray>(config.bootstrapServers)
            kafkaConsumer.subscribe(listOf(topic))
            val count = kafkaConsumer.poll(5000).count()
            kafkaConsumer.close()
            count
        }
        db.setConfig("streams.source.topic.nodes.neo4j-product", "Product{name, code}")
                .setConfig("streams.source.topic.nodes.neo4j-color", "Color{*}")
                .setConfig("streams.source.topic.nodes.neo4j-basket", "Basket{*}")
                .setConfig("streams.source.topic.relationships.neo4j-isin", "IS_IN{month,day}")
                .setConfig("streams.source.topic.relationships.neo4j-hascolor", "HAS_COLOR{*}")
                .start()
        val config = KafkaConfiguration(bootstrapServers = KafkaEventRouterSuiteIT.kafka.bootstrapServers)
        db.execute("""
            CREATE (p:Product{id: "A1", code: "X1", name: "Name X1", price: 1000})-[:IS_IN{month:4, day:4, year:2018}]->(b:Basket{name:"Basket-A", created: "20181228"}),
	            (p)-[:HAS_COLOR]->(c:Color{name: "Red"})
        """.trimIndent())

        val recordsProduct = async { getRecordCount(config, "neo4j-product") }
        val recordsColor = async { getRecordCount(config, "neo4j-color") }
        val recordsBasket = async { getRecordCount(config, "neo4j-basket") }
        val recordsIsIn = async { getRecordCount(config, "neo4j-isin") }
        val recordsHasColor = async { getRecordCount(config, "neo4j-hascolor") }

        assertEquals(1, recordsProduct.await())
        assertEquals(1, recordsColor.await())
        assertEquals(1, recordsBasket.await())
        assertEquals(1, recordsIsIn.await())
        assertEquals(1, recordsHasColor.await())
    }

    @Test
    fun testCreateNodeWithFrom() {
        db.setConfig("streams.source.topic.nodes.fromTopic.from.neo4j", "Person : Neo4j{*}")
                .start()
        kafkaConsumer.subscribe(listOf("fromTopic"))
        db.execute("CREATE (:Person:Neo4j {name:'John Doe', age:42})")
        val records = kafkaConsumer.poll(5000)
        assertEquals(1, records.count())
        assertEquals(true, records.all {
            JSONUtils.asStreamsTransactionEvent(it.value()).let {
                val after = it.payload.after as NodeChange
                val labels = after.labels
                val propertiesAfter = after.properties
                labels == listOf("Person", "Neo4j") && propertiesAfter == mapOf("name" to "John Doe", "age" to 42)
                        && it.meta.operation == OperationType.created
                        && it.schema.properties == mapOf("name" to "String", "age" to "Long")
                        && it.schema.constraints.isEmpty()
            }
        })
    }

    @Test
    fun testDeleteNode() = runBlocking {
        val topic = "testdeletetopic"
        db.setConfig("streams.source.topic.nodes.$topic.from.neo4j", "Person:Neo4j{*}")
            .setConfig("streams.source.topic.relationships.$topic.from.neo4j", "KNOWS{*}")
            .start()
        kafkaConsumer.subscribe(listOf(topic))
        db.execute("CREATE (:Person:ToRemove {name:'John Doe', age:42})-[:KNOWS]->(:Person {name:'Jane Doe', age:36})")
        Assert.assertEventually(ThrowingSupplier {
            kafkaConsumer.poll(5000).count() > 0
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
        db.execute("MATCH (p:Person {name:'John Doe', age:42}) REMOVE p:ToRemove")
        Assert.assertEventually(ThrowingSupplier {
            kafkaConsumer.poll(5000)
                    .map { JSONUtils.asStreamsTransactionEvent(it.value()) }
                    .filter { it.meta.operation == OperationType.updated }
                    .count() > 0
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
        db.execute("MATCH (p:Person) DETACH DELETE p")
        val count = db.execute("MATCH (p:Person {name:'John Doe', age:42}) RETURN count(p) AS count") {
            it.columnAs<Long>("count").next()
        }
        assertEquals(0, count)
        Assert.assertEventually(ThrowingSupplier {
            val count = kafkaConsumer.poll(5000)
                .map { JSONUtils.asStreamsTransactionEvent(it.value()) }
                .filter { it.meta.operation == OperationType.deleted }
                .count()
            count > 0
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun testIssue382() {
        val topic = UUID.randomUUID().toString()
        db.setConfig("streams.source.topic.nodes.$topic", "Label:`labels::label`{*}").start()
        kafkaConsumer.subscribe(listOf(topic))
        db.execute("CREATE (:Label:`labels::label` {name:'John Doe', age:42})")
        val records = kafkaConsumer.poll(5000)
        assertEquals(1, records.count())

        assertTrue { records.all {
            JSONUtils.asStreamsTransactionEvent(it.value()).let {
                val after = it.payload.after as NodeChange
                val labels = after.labels
                val propertiesAfter = after.properties
                labels == listOf("Label", "labels::label") && propertiesAfter == mapOf("name" to "John Doe", "age" to 42)
                        && it.meta.operation == OperationType.created
                        && it.schema.properties == mapOf("name" to "String", "age" to "Long")
                        && it.schema.constraints.isEmpty()
            }
        }}
    }

    @Test
    fun testCreateNodeWithMultiplePatternAndWithMultipeTwoPointsAndWhiteSpaces() {
        val topic = UUID.randomUUID().toString()
        db.setConfig("streams.source.topic.nodes.$topic", "Label  :  ` lorem  : ipsum : dolor : sit `{name, surname}").start()
        kafkaConsumer.subscribe(listOf(topic))
        db.execute("CREATE (:Label:` lorem  : ipsum : dolor : sit ` {name:'John', surname:'Doe', age: 42})")
        val records = kafkaConsumer.poll(5000)
        assertEquals(1, records.count())

        assertTrue { records.all {
            JSONUtils.asStreamsTransactionEvent(it.value()).let {
                val after = it.payload.after as NodeChange
                val labels = after.labels
                val propertiesAfter = after.properties
                labels == listOf("Label", " lorem  : ipsum : dolor : sit ") && propertiesAfter == mapOf("name" to "John", "surname" to "Doe")
                        && it.meta.operation == OperationType.created
                        && it.schema.properties == mapOf("name" to "String", "surname" to "String", "age" to "Long")
                        && it.schema.constraints.isEmpty()
            }
        }}
    }

    @Test
    fun testCreateNodeWithSinglePatternAndWithMultipeTwoPoints() {
        val topic = UUID.randomUUID().toString()
        db.setConfig("streams.source.topic.nodes.$topic", "` lorem:ipsum:dolor:sit `{*}").start()
        kafkaConsumer.subscribe(listOf(topic))
        db.execute("CREATE (:` lorem:ipsum:dolor:sit ` {name:'John Doe', age:42})")
        val records = kafkaConsumer.poll(5000)
        assertEquals(1, records.count())

        assertTrue { records.all {
            JSONUtils.asStreamsTransactionEvent(it.value()).let {
                val after = it.payload.after as NodeChange
                val labels = after.labels
                val propertiesAfter = after.properties
                labels == listOf(" lorem:ipsum:dolor:sit ") && propertiesAfter == mapOf("name" to "John Doe", "age" to 42)
                        && it.meta.operation == OperationType.created
                        && it.schema.properties == mapOf("name" to "String", "age" to "Long")
                        && it.schema.constraints.isEmpty()
            }
        }}
    }

    @Test
    fun testCreateRelWithBacktickPattern() {
        val topic = UUID.randomUUID().toString()
        db.setConfig("streams.source.topic.relationships.$topic", "`KNOWS::VERY:WELL`{*}").start()
        kafkaConsumer.subscribe(listOf(topic))
        db.execute("CREATE (:NoteTest {name:'Foo'})-[:`KNOWS::VERY:WELL`{since: 2014}]->(:NoteTest {name:'Bar'})")
        val records = kafkaConsumer.poll(5000)
        assertEquals(1, records.count())
        assertTrue { records.all {
            JSONUtils.asStreamsTransactionEvent(it.value()).let {
                val payload = it.payload as RelationshipPayload
                val properties = payload.after!!.properties!!
                payload.type == EntityType.relationship && payload.label == "KNOWS::VERY:WELL"
                        && properties["since"] == 2014
                        && it.schema.properties == mapOf("since" to "Long")
                        && it.schema.constraints.isEmpty()
            }
        }}
    }
}