package streams.integrations

import extension.newDatabase
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.hamcrest.Matchers
import org.junit.Test
import org.neo4j.function.ThrowingSupplier
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.kernel.internal.GraphDatabaseAPI
import streams.events.Constraint
import streams.events.EntityType
import streams.events.NodeChange
import streams.events.NodePayload
import streams.events.OperationType
import streams.events.RelKeyStrategy
import streams.events.RelationshipPayload
import streams.events.StreamsConstraintType
import streams.events.StreamsEvent
import streams.kafka.KafkaConfiguration
import streams.kafka.KafkaTestUtils.createConsumer
import streams.serialization.JSONUtils
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.assertNotNull
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith


@Suppress("UNCHECKED_CAST", "DEPRECATION")
class KafkaEventRouterIT: KafkaEventRouterBaseIT() {

    private fun initDbWithConfigs(configs: Map<String, String>) {
        configs.forEach { (k, v) -> graphDatabaseBuilder.setConfig(k, v) }
        db.shutdown()
        db = graphDatabaseBuilder.newDatabase() as GraphDatabaseAPI
    }

    private fun initDbWithConfigsAndConstraints(configs: Map<String, String>, constraints: List<String>) {
        graphDatabaseBuilder.setConfig("streams.source.schema.polling.interval", "0")
        initDbWithConfigs(configs)
        constraints.forEach { db.execute(it).close() }
    }

    @Test
    fun testCreateNode() {
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        val consumer = createConsumer(config)
        consumer.subscribe(listOf("neo4j"))
        db.execute("CREATE (:Person {name:'John Doe', age:42})").close()
        val records = consumer.poll(5000)
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
        consumer.close()
    }

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

        val configs = mapOf("streams.source.topic.nodes.$personTopic" to "$labelStart{*}",
                "streams.source.topic.nodes.$productTopic" to "$labelEnd{*}",
                "streams.source.topic.relationships.$topicWithStrategyAll" to "$keyStrategyAll{*}",
                "streams.source.topic.relationships.$topicWithStrategyDefault" to "$keyStrategyDefault{*}",
                "streams.source.topic.relationships.$topicWithoutStrategy" to "$noKeyStrategy{*}",
                "streams.source.topic.relationships.$topicWithStrategyAll.key_strategy" to RelKeyStrategy.ALL.toString().toLowerCase(),
                "streams.source.topic.relationships.$topicWithStrategyDefault.key_strategy" to RelKeyStrategy.DEFAULT.toString().toLowerCase())

        val constraints = listOf("CREATE CONSTRAINT ON (p:$labelStart) ASSERT p.name IS UNIQUE",
                "CREATE CONSTRAINT ON (p:$labelStart) ASSERT p.surname IS UNIQUE",
                "CREATE CONSTRAINT ON (p:$labelEnd) ASSERT p.name IS UNIQUE")

        initDbWithConfigsAndConstraints(configs, constraints)

        val expectedSetConstraints = setOf(
                Constraint(labelStart, setOf("name"), StreamsConstraintType.UNIQUE),
                Constraint(labelStart, setOf("surname"), StreamsConstraintType.UNIQUE),
                Constraint(labelEnd, setOf("name"), StreamsConstraintType.UNIQUE)
        )
        val expectedPropsAllKeyStrategy = mapOf("name" to "Foo", "surname" to "Bar")
        val expectedPropsDefaultKeyStrategy = mapOf("name" to "Foo")
        val expectedEndProps = mapOf("name" to "One")

        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)

        // we test key_strategy=all with create/update/delete relationship
        createConsumer(config).use { consumer ->
            consumer.subscribe(listOf(topicWithStrategyAll))
            db.execute("""
                |MERGE (p:$labelStart {name:'Foo', surname: 'Bar', address: 'Earth'})
                |MERGE (pp:$labelEnd {name:'One', price: '100€'})
                |MERGE (p)-[:$keyStrategyAll]->(pp)
            """.trimMargin()).close()
            val records = consumer.poll(5000)
            assertEquals(1, records.count())
            val record = records.first()
            assertNotNull(JSONUtils.readValue<Any>(record.key()))
            val valueCreate = JSONUtils.asStreamsTransactionEvent(record.value())
            val payload = valueCreate.payload as RelationshipPayload
            val (start, end, setConstraints) = Triple(payload.start, payload.end, valueCreate.schema.constraints.toSet())
            assertEquals(start.ids, expectedPropsAllKeyStrategy)
            assertEquals(end.ids, expectedEndProps)
            assertEquals(setConstraints, expectedSetConstraints)
            assertTrue(isValidRelationship(valueCreate, OperationType.created))

            db.execute("MATCH (p)-[rel:$keyStrategyAll]->(pp) SET rel.type = 'update'").close()
            val updatedRecords = consumer.poll(5000)
            assertEquals(1, updatedRecords.count())
            val updatedRecord = updatedRecords.first()
            assertNotNull(JSONUtils.readValue<Any>(updatedRecord.key()))
            val valueUpdate = JSONUtils.asStreamsTransactionEvent(updatedRecords.first().value())
            val payloadUpdate = valueUpdate.payload as RelationshipPayload
            val (startUpdate, endUpdate, setConstraintsUpdate) = Triple(payloadUpdate.start, payloadUpdate.end, valueUpdate.schema.constraints.toSet())
            assertEquals(expectedPropsAllKeyStrategy, startUpdate.ids)
            assertEquals(expectedEndProps, endUpdate.ids)
            assertEquals(setConstraintsUpdate, setConstraintsUpdate)
            assertTrue(isValidRelationship(valueUpdate, OperationType.updated))

            db.execute("MATCH (p)-[rel:$keyStrategyAll]->(pp) DELETE rel").close()
            val deletedRecords = consumer.poll(5000)
            assertEquals(1, deletedRecords.count())
            val deletedRecord = deletedRecords.first()
            assertNotNull(JSONUtils.readValue<Any>(deletedRecord.key()))
            val valueDelete = JSONUtils.asStreamsTransactionEvent(deletedRecords.first().value())
            val payloadDelete = valueDelete.payload as RelationshipPayload
            val (startDelete, endDelete, setConstraintsDelete) = Triple(payloadDelete.start, payloadDelete.end, valueDelete.schema.constraints.toSet())
            assertEquals(expectedPropsAllKeyStrategy, startDelete.ids)
            assertEquals(expectedEndProps, endDelete.ids)
            assertEquals(expectedSetConstraints, setConstraintsDelete)
            assertTrue(isValidRelationship(valueDelete, OperationType.deleted))
        }

        // we test key_strategy=default with create/update/delete relationship
        createConsumer(config).use { consumer ->
            consumer.subscribe(listOf(topicWithStrategyDefault))
            db.execute("""
                |MERGE (p:$labelStart {name:'Foo', surname: 'Bar', address: 'Earth'})
                |MERGE (pp:$labelEnd {name:'One', price: '100€'})
                |MERGE (p)-[:$keyStrategyDefault]->(pp)
            """.trimMargin()).close()
            val records = consumer.poll(5000)
            assertEquals(1, records.count())
            val record = records.first()
            assertNotNull(JSONUtils.readValue<Any>(record.key()))
            val valueCreate = JSONUtils.asStreamsTransactionEvent(record.value())
            val payload = valueCreate.payload as RelationshipPayload
            val (start, end, setConstraints) = Triple(payload.start, payload.end, valueCreate.schema.constraints.toSet())
            assertEquals(expectedPropsDefaultKeyStrategy, start.ids)
            assertEquals(expectedEndProps, end.ids)
            assertEquals(expectedSetConstraints, setConstraints)
            assertTrue(isValidRelationship(valueCreate, OperationType.created))

            db.execute("MATCH (p)-[rel:$keyStrategyDefault]->(pp) SET rel.type = 'update'").close()
            val updatedRecords = consumer.poll(5000)
            assertEquals(1, updatedRecords.count())
            val updatedRecord = updatedRecords.first()
            assertNotNull(JSONUtils.readValue<Any>(updatedRecord.key()))
            val valueUpdate = JSONUtils.asStreamsTransactionEvent(updatedRecords.first().value())
            val payloadUpdate = valueUpdate.payload as RelationshipPayload
            val (startUpdate, endUpdate, setConstraintsUpdate) = Triple(payloadUpdate.start, payloadUpdate.end, valueUpdate.schema.constraints.toSet())
            assertEquals(expectedPropsDefaultKeyStrategy, startUpdate.ids)
            assertEquals(expectedEndProps, endUpdate.ids)
            assertEquals(expectedSetConstraints, setConstraintsUpdate)
            assertTrue(isValidRelationship(valueUpdate, OperationType.updated))

            db.execute("MATCH (p)-[rel:$keyStrategyDefault]->(pp) DELETE rel").close()
            val deletedRecords = consumer.poll(5000)
            assertEquals(1, deletedRecords.count())
            val deletedRecord = deletedRecords.first()
            assertNotNull(JSONUtils.readValue<Any>(deletedRecord.key()))
            val valueDelete = JSONUtils.asStreamsTransactionEvent(deletedRecords.first().value())
            val payloadDelete = valueDelete.payload as RelationshipPayload
            val (startDelete, endDelete, setConstraintsDelete) = Triple(payloadDelete.start, payloadDelete.end, valueDelete.schema.constraints.toSet())
            assertEquals(expectedPropsDefaultKeyStrategy, startDelete.ids)
            assertEquals(expectedEndProps, endDelete.ids)
            assertEquals(expectedSetConstraints, setConstraintsDelete)
            assertTrue(isValidRelationship(valueDelete, OperationType.deleted))
        }

        // we test a topic without key_strategy (that is, 'default') with create/update/delete relationship
        createConsumer(config).use { consumer ->
            consumer.subscribe(listOf(topicWithoutStrategy))
            db.execute("""
                |MERGE (p:$labelStart {name:'Foo', surname: 'Bar', address: 'Earth'})
                |MERGE (pp:$labelEnd {name:'One', price: '100€'})
                |MERGE (p)-[:$noKeyStrategy]->(pp)
            """.trimMargin()).close()
            val records = consumer.poll(5000)
            assertEquals(1, records.count())
            val record = records.first()
            assertNotNull(JSONUtils.readValue<Any>(record.key()))
            val valueCreate = JSONUtils.asStreamsTransactionEvent(record.value())
            val payload = valueCreate.payload as RelationshipPayload
            val (start, end, setConstraints) = Triple(payload.start, payload.end, valueCreate.schema.constraints.toSet())
            assertEquals(expectedPropsDefaultKeyStrategy, start.ids)
            assertEquals(expectedEndProps, end.ids)
            assertEquals(expectedSetConstraints, setConstraints)
            assertTrue(isValidRelationship(valueCreate, OperationType.created))

            db.execute("MATCH (p)-[rel:$noKeyStrategy]->(pp) SET rel.type = 'update'").close()
            val updatedRecords = consumer.poll(10000)
            assertEquals(1, updatedRecords.count())
            val updatedRecord = updatedRecords.first()
            assertNotNull(JSONUtils.readValue<Any>(updatedRecord.key()))
            val valueUpdate = JSONUtils.asStreamsTransactionEvent(updatedRecords.first().value())
            val payloadUpdate = valueUpdate.payload as RelationshipPayload
            val (startUpdate, endUpdate, setConstraintsUpdate) = Triple(payloadUpdate.start, payloadUpdate.end, valueUpdate.schema.constraints.toSet())
            assertEquals(expectedPropsDefaultKeyStrategy, startUpdate.ids)
            assertEquals(expectedEndProps, endUpdate.ids)
            assertEquals(expectedSetConstraints, setConstraintsUpdate)
            assertTrue(isValidRelationship(valueUpdate, OperationType.updated))

            db.execute("MATCH (p)-[rel:$noKeyStrategy]->(pp) DELETE rel")
            val deletedRecords = consumer.poll(10000)
            assertEquals(1, deletedRecords.count())
            val deletedRecord = deletedRecords.first()
            assertNotNull(JSONUtils.readValue<Any>(deletedRecord.key()))
            val valueDelete = JSONUtils.asStreamsTransactionEvent(deletedRecords.first().value())
            val payloadDelete = valueDelete.payload as RelationshipPayload
            val (startDelete, endDelete, setConstraintsDelete) = Triple(payloadDelete.start, payloadDelete.end, valueDelete.schema.constraints.toSet())
            assertEquals(expectedPropsDefaultKeyStrategy, startDelete.ids)
            assertEquals(expectedEndProps, endDelete.ids)
            assertEquals(expectedSetConstraints, setConstraintsDelete)
            assertTrue(isValidRelationship(valueDelete, OperationType.deleted))
        }
    }

    @Test
    fun testCreateRelationshipWithRelRouting() {
        initDbWithConfigs(mapOf("streams.source.topic.relationships.knows" to "KNOWS{*}"))

        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        val consumer = createConsumer(config)
        consumer.subscribe(listOf("knows"))
        db.execute("CREATE (:Person {name:'Andrea'})-[:KNOWS{since: 2014}]->(:Person {name:'Michael'})").close()
        val records = consumer.poll(5000)
        assertEquals(1, records.count())
        assertEquals(true, records.all {
            JSONUtils.asStreamsTransactionEvent(it.value()).let {
                val payload = it.payload as RelationshipPayload
                val properties = payload.after!!.properties!!
                payload.type == EntityType.relationship && payload.label == "KNOWS"
                        && properties["since"] == 2014
                        && it.schema.properties == mapOf("since" to "Long")
                        && it.schema.constraints.isEmpty()
            }
        })
        consumer.close()
    }

    @Test
    fun testCreateNodeWithNodeRouting() {
        initDbWithConfigs(mapOf("streams.source.topic.nodes.person" to "Person{*}"))
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        val consumer = createConsumer(config)
        consumer.subscribe(listOf("person"))
        db.execute("CREATE (:Person {name:'Andrea'})").close()
        val records = consumer.poll(5000)
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
        consumer.close()
    }

    @Test
    fun testProcedure() {
        val topic = UUID.randomUUID().toString()
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        val consumer = createConsumer(config)
        consumer.subscribe(listOf(topic))
        val message = "Hello World"
        db.execute("CALL streams.publish('$topic', '$message')").close()
        val records = consumer.poll(5000)
        assertEquals(1, records.count())
        assertTrue { records.all {
            JSONUtils.readValue<StreamsEvent>(it.value()).let {
                message == it.payload
            }
        }}
        consumer.close()
    }

    @Test
    fun testProcedureWithKey() {
        val topic = UUID.randomUUID().toString()
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        createConsumer(config).use {
            it.subscribe(listOf(topic))
            val message = "Hello World"
            val keyRecord = "test"
            db.execute("CALL streams.publish('$topic', '$message', {key: '$keyRecord'} )").close()
            val records = it.poll(5000)
            assertEquals(1, records.count())
            assertTrue { records.all {
                JSONUtils.readValue<StreamsEvent>(it.value()).payload == message
                        && JSONUtils.readValue<String>(it.key()) == keyRecord
            }}
        }
    }

    @Test
    fun testProcedureWithKeyAsMap() {
        val topic = UUID.randomUUID().toString()
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        createConsumer(config).use {
            it.subscribe(listOf(topic))
            val message = "Hello World"
            val keyRecord = mutableMapOf("one" to "Foo", "two" to "Baz", "three" to "Bar")
            db.execute("CALL streams.publish('$topic', '$message', {key: \$key } )", mapOf("key" to keyRecord)).close()
            val records = it.poll(5000)
            assertEquals(1, records.count())
            assertTrue { records.all {
                JSONUtils.readValue<StreamsEvent>(it.value()).payload == message
                        && JSONUtils.readValue<Map<String, String>>(it.key()) == keyRecord
            }}
        }
    }

    @Test
    fun testProcedureWithPartitionAsNotNumber() {
        val topic = UUID.randomUUID().toString()
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        createConsumer(config).use {
            it.subscribe(listOf(topic))
            val message = "Hello World"
            val keyRecord = "test"
            val partitionRecord = "notNumber"
            assertFailsWith(QueryExecutionException::class) {
                db.execute("CALL streams.publish('$topic', '$message', {key: '$keyRecord', partition: '$partitionRecord' })").close()
            }
        }
    }

    @Test
    fun testProcedureWithPartitionAndKey() {
        val topic = UUID.randomUUID().toString()
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        createConsumer(config).use {
            it.subscribe(listOf(topic))
            val message = "Hello World"
            val keyRecord = "test"
            val partitionRecord = 0
            db.execute("CALL streams.publish('$topic', '$message', {key: '$keyRecord', partition: $partitionRecord })").close()
            val records = it.poll(5000)
            assertEquals(1, records.count())
            assertTrue{ records.all {
                JSONUtils.readValue<StreamsEvent>(it.value()).payload == message
                        && JSONUtils.readValue<String>(it.key()) == keyRecord
                        && partitionRecord == it.partition()
            }}
        }
    }

    @Test
    fun testCantPublishNull() {
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        createConsumer(config).use { consumer ->
            consumer.subscribe(listOf("neo4j"))

            assertFailsWith(RuntimeException::class) {
                db.execute("CALL streams.publish('neo4j', null)")
            }
        }
    }

    @Test
    fun testProcedureSyncWithNode() {
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        createConsumer(config).use { consumer ->

            consumer.subscribe(listOf("neo4j"))
            db.execute("MATCH (n:Baz) DETACH DELETE n").close()
            db.execute("CREATE (n:Baz {age: 23, name: 'Foo', surname: 'Bar'})").close()

            val recordsCreation = consumer.poll(5000)
            assertEquals(1, recordsCreation.count())

            db.execute("MATCH (n:Baz) \n" +
                    "CALL streams.publish.sync('neo4j', n) \n" +
                    "YIELD value \n" +
                    "RETURN value").use { result ->
                assertTrue { result.hasNext() }
                val resultMap = (result.next())["value"] as Map<String, Any>

                assertNotNull(resultMap["offset"])
                assertNotNull(resultMap["partition"])
                assertNotNull(resultMap["keySize"])
                assertNotNull(resultMap["valueSize"])
                assertNotNull(resultMap["timestamp"])

                assertFalse { result.hasNext() }
            }

            val records = consumer.poll(5000)
            assertEquals(1, records.count())
            assertEquals(3, ((records.map {
                JSONUtils.readValue<StreamsEvent>(it.value()).payload
            }[0] as Map<String, Any>)["properties"] as Map<String, Any>).size)
        }
    }

    @Test
    fun testProcedureSync() {
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        createConsumer(config).use { consumer ->
            consumer.subscribe(listOf("syncTopic"))
            val message = "Hello World"
            db.execute("CALL streams.publish.sync('syncTopic', '$message')").use { result ->
                assertTrue { result.hasNext() }
                val resultMap = (result.next())["value"] as Map<String, Any>
                assertNotNull(resultMap["offset"])
                assertNotNull(resultMap["partition"])
                assertNotNull(resultMap["keySize"])
                assertNotNull(resultMap["valueSize"])
                assertNotNull(resultMap["timestamp"])

                assertFalse { result.hasNext() }
            }

            val records = consumer.poll(5000)
            assertEquals(1, records.count())
            assertEquals(true, records.all {
                JSONUtils.readValue<StreamsEvent>(it.value()).payload == message
            })
        }
    }

    private fun getRecordCount(config: KafkaConfiguration, topic: String): Int {
        val consumer = createConsumer(config)
        consumer.subscribe(listOf(topic))
        val count = consumer.poll(5000).count()
        consumer.close()
        return count
    }

    @Test
    fun testMultiTopicPatternConfig() = runBlocking {
        val configsMap = mapOf("streams.source.topic.nodes.neo4j-product" to "Product{name, code}",
                "streams.source.topic.nodes.neo4j-color" to "Color{*}",
                "streams.source.topic.nodes.neo4j-basket" to "Basket{*}",
                "streams.source.topic.relationships.neo4j-isin" to "IS_IN{month,day}",
                "streams.source.topic.relationships.neo4j-hascolor" to "HAS_COLOR{*}")
        initDbWithConfigs(configsMap)
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        db.execute("""
            CREATE (p:Product{id: "A1", code: "X1", name: "Name X1", price: 1000})-[:IS_IN{month:4, day:4, year:2018}]->(b:Basket{name:"Basket-A", created: "20181228"}),
	            (p)-[:HAS_COLOR]->(c:Color{name: "Red"})
        """.trimIndent()).close()

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
    fun testCreateNodeWithConstraints() {
        val configs = mapOf("streams.source.topic.nodes.personConstraints" to "PersonConstr{*}",
                "streams.source.topic.nodes.productConstraints" to "ProductConstr{*}",
                "streams.source.topic.relationships.boughtConstraints" to "BOUGHT{*}")
        val constraints = listOf("CREATE CONSTRAINT ON (p:PersonConstr) ASSERT p.name IS UNIQUE",
                "CREATE CONSTRAINT ON (p:ProductConstr) ASSERT p.name IS UNIQUE")
        initDbWithConfigsAndConstraints(configs, constraints)
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        val consumer = createConsumer(config)
        consumer.subscribe(listOf("personConstraints"))
        db.execute("CREATE (:PersonConstr {name:'Andrea'})").close()
        val records = consumer.poll(5000)
        assertEquals(1, records.count())
        assertEquals(true, records.all {
            JSONUtils.asStreamsTransactionEvent(it.value()).let {
                val payload = it.payload as NodePayload
                val labels = payload.after!!.labels!!
                val properties = payload.after!!.properties
                labels == listOf("PersonConstr") && properties == mapOf("name" to "Andrea")
                        && it.meta.operation == OperationType.created
                        && it.schema.properties == mapOf("name" to "String")
                        && it.schema.constraints == listOf(Constraint("PersonConstr", setOf("name"), StreamsConstraintType.UNIQUE))
            }
        })
        consumer.close()
    }

    @Test
    fun testCreateRelationshipWithConstraints() {
        val configs = mapOf("streams.source.topic.nodes.personConstraints" to "PersonConstr{*}",
                "streams.source.topic.nodes.productConstraints" to "ProductConstr{*}",
                "streams.source.topic.relationships.boughtConstraints" to "BOUGHT{*}")
        val constraints = listOf("CREATE CONSTRAINT ON (p:PersonConstr) ASSERT p.name IS UNIQUE",
                "CREATE CONSTRAINT ON (p:ProductConstr) ASSERT p.name IS UNIQUE")
        initDbWithConfigsAndConstraints(configs, constraints)
        db.execute("CREATE (:PersonConstr {name:'Andrea'})").close()
        db.execute("CREATE (:ProductConstr {name:'My Awesome Product', price: '100€'})").close()
        db.execute("""
            |MATCH (p:PersonConstr {name:'Andrea'})
            |MATCH (pp:ProductConstr {name:'My Awesome Product'})
            |MERGE (p)-[:BOUGHT]->(pp)
        """.trimMargin())
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        val consumer = createConsumer(config)
        consumer.subscribe(listOf("personConstraints", "productConstraints", "boughtConstraints"))
        val records = consumer.poll(10000)
        assertEquals(3, records.count())

        val map = records
                .map {
                    val evt = JSONUtils.asStreamsTransactionEvent(it.value())
                    evt.payload.type to evt
                }
                .groupBy({ it.first }, { it.second })
        assertEquals(true, map[EntityType.node].orEmpty().isNotEmpty() && map[EntityType.node].orEmpty().all {
            val payload = it.payload as NodePayload
            val (labels, properties) = payload.after!!.labels!! to payload.after!!.properties!!
            when (labels) {
                listOf("ProductConstr") -> properties == mapOf("name" to "My Awesome Product", "price" to "100€")
                        && it.meta.operation == OperationType.created
                        && it.schema.properties == mapOf("name" to "String", "price" to "String")
                        && it.schema.constraints == listOf(Constraint("ProductConstr", setOf("name"), StreamsConstraintType.UNIQUE))
                listOf("PersonConstr") -> properties == mapOf("name" to "Andrea")
                        && it.meta.operation == OperationType.created
                        && it.schema.properties == mapOf("name" to "String")
                        && it.schema.constraints == listOf(Constraint("PersonConstr", setOf("name"), StreamsConstraintType.UNIQUE))
                else -> false
            }
        })
        assertEquals(true, map[EntityType.relationship].orEmpty().isNotEmpty() && map[EntityType.relationship].orEmpty().all {
            val payload = it.payload as RelationshipPayload
            val (start, end, properties) = Triple(payload.start, payload.end, payload.after!!.properties!!)
            properties.isNullOrEmpty()
                    && start.ids == mapOf("name" to "Andrea")
                    && end.ids == mapOf("name" to "My Awesome Product")
                    && it.meta.operation == OperationType.created
                    && it.schema.properties == emptyMap<String, String>()
                    && it.schema.constraints.toSet() == setOf(
                            Constraint("PersonConstr", setOf("name"), StreamsConstraintType.UNIQUE),
                            Constraint("ProductConstr", setOf("name"), StreamsConstraintType.UNIQUE))
        })
        consumer.close()
    }

    @Test
    fun testDeleteNodeWithTestDeleteTopic() {
        val configsMap = mapOf("streams.source.topic.nodes.testdeletetopic" to "Person:Neo4j{*}",
                "streams.source.topic.relationships.testdeletetopic" to "KNOWS{*}")
        initDbWithConfigs(configsMap)
        val topic = "testdeletetopic"
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        val kafkaConsumer = createConsumer(config)
        kafkaConsumer.subscribe(listOf(topic))
        db.execute("CREATE (:Person:ToRemove {name:'John Doe', age:42})-[:KNOWS]->(:Person {name:'Jane Doe', age:36})")
        org.neo4j.test.assertion.Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            kafkaConsumer.poll(5000).count() > 0
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
        db.execute("MATCH (p:Person {name:'John Doe', age:42}) REMOVE p:ToRemove")
        org.neo4j.test.assertion.Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            kafkaConsumer.poll(5000)
                    .map { JSONUtils.asStreamsTransactionEvent(it.value()) }
                    .filter { it.meta.operation == OperationType.updated }
                    .count() > 0
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
        db.execute("MATCH (p:Person) DETACH DELETE p")
        val count = db.execute("MATCH (p:Person {name:'John Doe', age:42}) RETURN count(p) AS count")
                .columnAs<Long>("count").next()
        assertEquals(0, count)
        org.neo4j.test.assertion.Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            kafkaConsumer.poll(5000)
                    .map { JSONUtils.asStreamsTransactionEvent(it.value()) }
                    .filter { it.meta.operation == OperationType.deleted }
                    .count() > 0
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
        kafkaConsumer.close()
    }

    @Test
    fun testProcedureSyncWithKeyNull() {
        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        createConsumer(config).use { consumer ->

            val message = "Hello World"
            consumer.subscribe(listOf("neo4j"))
            db.execute("CREATE (n:Foo {id: 1, name: 'Bar'})").close()

            val recordsCreation = consumer.poll(5000)
            assertEquals(1, recordsCreation.count())

            db.execute("MATCH (n:Foo {id: 1}) CALL streams.publish.sync('neo4j', '$message', {key: n.foo}) YIELD value RETURN value").use { result ->
                assertTrue { result.hasNext() }
                val resultMap = (result.next())["value"] as Map<String, Any>

                assertNotNull(resultMap["offset"])
                assertNotNull(resultMap["partition"])
                assertNotNull(resultMap["keySize"])
                assertNotNull(resultMap["valueSize"])
                assertNotNull(resultMap["timestamp"])

                assertFalse { result.hasNext() }
            }

            val records = consumer.poll(5000)
            assertEquals(1, records.count())
            assertTrue { records.all {
                JSONUtils.readValue<StreamsEvent>(it.value()).payload == message
                        && it.key() == null
            }}
        }
    }

    @Test
    fun testProcedureSyncWithConfig() {
        AdminClient.create(mapOf("bootstrap.servers" to kafka.bootstrapServers)).use { admin ->
            val topic = UUID.randomUUID().toString()
            admin.createTopics(listOf(NewTopic(topic, 5, 1))).all().get()
            val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
            createConsumer(config).use { consumer ->
                consumer.subscribe(listOf(topic))
                val message = "Hello World"
                val keyRecord = "test"
                val partitionRecord = 1
                db.execute("CALL streams.publish.sync('$topic', '$message', {key: '$keyRecord', partition: $partitionRecord })").use {result ->
                    assertTrue { result.hasNext() }
                    val resultMap = (result.next())["value"] as Map<String, Any>
                    assertNotNull(resultMap["offset"])
                    assertEquals(partitionRecord, resultMap["partition"])
                    assertNotNull(resultMap["keySize"])
                    assertNotNull(resultMap["valueSize"])
                    assertNotNull(resultMap["timestamp"])
                    assertFalse { result.hasNext() }
                }
                val records = consumer.poll(5000)
                assertEquals(1, records.count())
                assertEquals(1, records.count { it.partition() == 1 })
                assertTrue{ records.all {
                    JSONUtils.readValue<StreamsEvent>(it.value()).payload == message
                            && JSONUtils.readValue<String>(it.key()) == keyRecord
                            && partitionRecord == it.partition()
                }}
            }
        }
    }

    @Test
    fun testProcedureWithTopicWithMultiplePartitionAndKey() {
        AdminClient.create(mapOf("bootstrap.servers" to kafka.bootstrapServers)).use { admin ->
            val topic = UUID.randomUUID().toString()
            admin.createTopics(listOf(NewTopic(topic, 3, 1))).all().get()
            val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
            createConsumer(config).use { consumer ->
                consumer.subscribe(listOf(topic))
                val message = "Hello World"
                val keyRecord = "test"
                val partitionRecord = 2
                db.execute("CALL streams.publish('$topic', '$message', {key: '$keyRecord', partition: $partitionRecord })")
                val records = consumer.poll(5000)
                assertEquals(1, records.count())
                assertEquals(1, records.count { it.partition() == 2 })
                assertTrue{ records.all {
                    JSONUtils.readValue<StreamsEvent>(it.value()).payload == message
                            && JSONUtils.readValue<String>(it.key()) == keyRecord
                            && partitionRecord == it.partition()
                }}
            }
        }
    }

    @Test
    fun testProcedureSendMessageToNotExistentPartition() {
        AdminClient.create(mapOf("bootstrap.servers" to kafka.bootstrapServers)).use { admin ->
            val topic = UUID.randomUUID().toString()
            admin.createTopics(listOf(NewTopic(topic, 3, 1))).all().get()
            val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
            createConsumer(config).use { consumer ->
                consumer.subscribe(listOf(topic))
                val message = "Hello World"
                val keyRecord = "test"
                val partitionRecord = 9
                db.execute("CALL streams.publish('$topic', '$message', {key: '$keyRecord', partition: $partitionRecord })")
                val records = consumer.poll(5000)
                assertEquals(0, records.count())
            }
        }
    }
}