package streams.integrations

import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.junit.Test
import streams.events.EntityType
import streams.events.NodeChange
import streams.events.NodePayload
import streams.events.OperationType
import streams.events.RelationshipPayload
import streams.extensions.execute
import streams.kafka.KafkaConfiguration
import streams.KafkaTestUtils
import streams.serialization.JSONUtils
import streams.setConfig
import streams.start
import kotlin.test.assertEquals

class KafkaEventRouterSimpleTSE: KafkaEventRouterBaseTSE() {

    @Test
    fun testCreateNode() {
        db.start()
        kafkaConsumer.subscribe(listOf("neo4j"))
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
        db.setConfig("streams.source.topic.nodes.fromTopic.from.neo4j", "Person:Neo4j{*}")
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
}