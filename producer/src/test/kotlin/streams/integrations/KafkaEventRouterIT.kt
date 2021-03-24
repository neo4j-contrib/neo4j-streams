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
import streams.RelKeyStrategy
import streams.events.Constraint
import streams.events.EntityType
import streams.events.NodeChange
import streams.events.NodePayload
import streams.events.OperationType
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
    fun testWithSingleTopicAndMultipleKeyStrategiesAndLabels() {
        val allTopic = UUID.randomUUID().toString()

        val allPropsRelType = "BOUGHT"

        val labelStart = "PersonConstr"
        val anotherLabelStart = "SubjectConstr"
        val labelEnd = "ProductConstr"

        val configs = mapOf("streams.source.topic.nodes.$allTopic" to "$labelStart{*}",
                "streams.source.topic.nodes.$allTopic" to "$labelStart:$anotherLabelStart:$labelEnd{*}",
                "streams.source.topic.relationships.$allTopic" to "$allPropsRelType{*}",
                "streams.source.topic.relationships.$allTopic.key_strategy" to RelKeyStrategy.all.toString())

        val constraints = listOf("CREATE CONSTRAINT ON (p:$labelStart) ASSERT p.name IS UNIQUE",
                "CREATE CONSTRAINT ON (p:$labelStart) ASSERT p.surname IS UNIQUE",
                "CREATE CONSTRAINT ON (p:$anotherLabelStart) ASSERT p.name IS UNIQUE",
                "CREATE CONSTRAINT ON (p:$anotherLabelStart) ASSERT p.another IS UNIQUE",
                "CREATE CONSTRAINT ON (p:$labelEnd) ASSERT p.name IS UNIQUE")

        initDbWithConfigsAndConstraints(configs, constraints)

        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        createConsumer(config).use { consumer ->
            consumer.subscribe(listOf(allTopic))

            db.execute("CREATE (:$labelStart:$anotherLabelStart {name:'Foo', surname: 'Bar', address: 'Earth', another: 'Alpha'})").close()
            db.execute("CREATE (:$labelEnd {name:'One', price: '100€'})").close()
            db.execute("""
                |MATCH (p:$labelStart {name:'Foo'})
                |MATCH (pp:$labelEnd {name:'One'})
                |MERGE (p)-[:$allPropsRelType]->(pp)
            """.trimMargin())

            val records = consumer.poll(20000)
            assertEquals(3, records.count())


            val setConstraints = setOf(
                    Constraint(labelStart, setOf("name"), StreamsConstraintType.UNIQUE),
                    Constraint(labelStart, setOf("surname"), StreamsConstraintType.UNIQUE),
                    Constraint(anotherLabelStart, setOf("name"), StreamsConstraintType.UNIQUE),
                    Constraint(anotherLabelStart, setOf("another"), StreamsConstraintType.UNIQUE),
                    Constraint(labelEnd, setOf("name"), StreamsConstraintType.UNIQUE)
            )
            assertTrue(records.all {
                val value = JSONUtils.asStreamsTransactionEvent(it.value())
                val key = JSONUtils.readValue<Any>(it.key())
                when (val payload = value.payload) {
                    is NodePayload -> {
                        val (properties, operation, schema) = Triple(payload.after!!.properties!!, value.meta.operation, value.schema)
                        when (payload.after!!.labels!!.toSet()) {
                            setOf(labelStart, anotherLabelStart) -> properties == mapOf("address" to "Earth", "another" to "Alpha", "name" to "Foo", "surname" to "Bar")
                                    && operation == OperationType.created
                                    && schema.properties == mapOf("address" to "String", "another" to "String", "name" to "String", "surname" to "String")
                                    && schema.constraints.toSet() == setOf(Constraint(labelStart, setOf("name"), StreamsConstraintType.UNIQUE),
                                    Constraint(labelStart, setOf("surname"), StreamsConstraintType.UNIQUE),
                                    Constraint(anotherLabelStart, setOf("name"), StreamsConstraintType.UNIQUE),
                                    Constraint(anotherLabelStart, setOf("another"), StreamsConstraintType.UNIQUE))

                            setOf(labelEnd) -> properties == mapOf("name" to "One", "price" to "100€")
                                    && operation == OperationType.created
                                    && value.schema.properties == mapOf("name" to "String", "price" to "String")
                                    && value.schema.constraints.toSet() == setOf(Constraint(labelEnd, setOf("name"), StreamsConstraintType.UNIQUE))

                            else -> {
                                false
                            }
                        }
                    }

                    is RelationshipPayload -> {
                        val (start, end, setConstraints) = Triple(payload.start, payload.end, value.schema.constraints.toSet())

                        start.ids == mapOf("another" to "Alpha", "name" to "Foo", "surname" to "Bar")
                                    && end.ids == mapOf("name" to "One")
                                    && setConstraints == setConstraints
                                    && key is String
                                    && commonRelAssertions(value)
                    }
                    else -> false
                }
            })

            db.execute("MATCH (p)-[rel:$allPropsRelType]->(pp) DELETE rel")

            val deletedRecords = consumer.poll(20000)
            assertEquals(1, deletedRecords.count())
            val record = deletedRecords.first()
            val key = JSONUtils.readValue<Any>(record.key())
            val value = JSONUtils.asStreamsTransactionEvent(deletedRecords.first().value())
            val payload = value.payload as RelationshipPayload
            val (start, end, constraints) = Triple(payload.start, payload.end, value.schema.constraints.toSet())
            assertTrue {
                start.ids == mapOf("another" to "Alpha", "name" to "Foo", "surname" to "Bar")
                        && end.ids == mapOf("name" to "One")
                        && constraints == setConstraints
                        && key is String
                        && value.payload.before!!.properties!!.isNullOrEmpty()
                        && value.payload.after == null
                        && value.schema.properties == emptyMap<String, String>()
                        && value.meta.operation == OperationType.deleted
            }

        }
    }

    @Test
    fun testWithMultipleKeyStrategies() {

        val allProps = "BOUGHT"
        val oneProp = "ONE_PROP"
        val defaultProp = "DEFAULT"

        val labelStart = "PersonConstr"
        val labelEnd = "ProductConstr"

        val personTopic = UUID.randomUUID().toString()
        val productTopic = UUID.randomUUID().toString()
        val topicWithStrategyAll = UUID.randomUUID().toString()
        val topicWithStrategyFirst = UUID.randomUUID().toString()
        val topicWithoutStrategy = UUID.randomUUID().toString()

        val configs = mapOf("streams.source.topic.nodes.$personTopic" to "$labelStart{*}",
                "streams.source.topic.nodes.$personTopic" to "$labelStart{*}",
                "streams.source.topic.nodes.$productTopic" to "$labelEnd{*}",
                "streams.source.topic.relationships.$topicWithStrategyAll" to "$allProps{*}",
                "streams.source.topic.relationships.$topicWithStrategyFirst" to "$oneProp{*}",
                "streams.source.topic.relationships.$topicWithoutStrategy" to "$defaultProp{*}",
                "streams.source.topic.relationships.$topicWithStrategyAll.key_strategy" to RelKeyStrategy.all.toString(),
                "streams.source.topic.relationships.$topicWithStrategyFirst.key_strategy" to RelKeyStrategy.first.toString())

        val constraints = listOf("CREATE CONSTRAINT ON (p:$labelStart) ASSERT p.name IS UNIQUE",
                "CREATE CONSTRAINT ON (p:$labelStart) ASSERT p.surname IS UNIQUE",
                "CREATE CONSTRAINT ON (p:$labelEnd) ASSERT p.name IS UNIQUE")

        initDbWithConfigsAndConstraints(configs, constraints)

        val config = KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)
        createConsumer(config).use { consumer ->
            consumer.subscribe(listOf(
                    personTopic, productTopic, topicWithStrategyAll, topicWithStrategyFirst, topicWithoutStrategy
            ))

            db.execute("CREATE (:$labelStart {name:'Foo', surname: 'Bar', address: 'Earth'})").close()
            db.execute("CREATE (:$labelEnd {name:'One', price: '100€'})").close()
            db.execute("""
                |MATCH (p:$labelStart {name:'Foo'})
                |MATCH (pp:$labelEnd {name:'One'})
                |MERGE (p)-[:$allProps]->(pp)
            """.trimMargin()).close()
            db.execute("""
                |MATCH (p:$labelStart {name:'Foo'})
                |MATCH (pp:$labelEnd {name:'One'})
                |MERGE (p)-[:$oneProp]->(pp)
            """.trimMargin()).close()
            db.execute("""
                |MATCH (p:$labelStart {name:'Foo'})
                |MATCH (pp:$labelEnd {name:'One'})
                |MERGE (p)-[:$defaultProp]->(pp)
            """.trimMargin()).close()

            val recordsEntitiesCreated = consumer.poll(20000)
            assertEquals(5, recordsEntitiesCreated.count())

            val expectedSetConstraints = setOf(
                    Constraint(labelStart, setOf("name"), StreamsConstraintType.UNIQUE),
                    Constraint(labelStart, setOf("surname"), StreamsConstraintType.UNIQUE),
                    Constraint(labelEnd, setOf("name"), StreamsConstraintType.UNIQUE)
            )
            val expectedPropsAllKeyStrategy = mapOf("name" to "Foo", "surname" to "Bar")
            val expectedPropsFirstKeyStrategy = mapOf("name" to "Foo")
            val expectedEndProps = mapOf("name" to "One")

            assertTrue(recordsEntitiesCreated.all {
                val value = JSONUtils.asStreamsTransactionEvent(it.value())
                val key = JSONUtils.readValue<Any>(it.key())
                when (val payload = value.payload) {
                    is NodePayload -> {
                        val (properties, operation, schema) = Triple(payload.after!!.properties!!, value.meta.operation, value.schema)
                        when (payload.after!!.labels!!) {
                            listOf(labelStart) -> properties == mapOf("address" to "Earth", "name" to "Foo", "surname" to "Bar")
                                    && operation == OperationType.created
                                    && schema.properties == mapOf("address" to "String", "name" to "String", "surname" to "String")
                                    && schema.constraints.toSet() == setOf(Constraint(labelStart, setOf("name"), StreamsConstraintType.UNIQUE),
                                    Constraint(labelStart, setOf("surname"), StreamsConstraintType.UNIQUE))
                                    && key is String

                            listOf(labelEnd) -> properties == mapOf("name" to "One", "price" to "100€")
                                    && operation == OperationType.created
                                    && value.schema.properties == mapOf("name" to "String", "price" to "String")
                                    && value.schema.constraints.toSet() == setOf(Constraint(labelEnd, setOf("name"), StreamsConstraintType.UNIQUE))
                                    && key is String

                            else -> {
                                false
                            }
                        }
                    }
                    is RelationshipPayload -> {
                        val (start, end, setConstraints) = Triple(payload.start, payload.end, value.schema.constraints.toSet())
                        when (payload.label) {
                            allProps -> start.ids == expectedPropsAllKeyStrategy
                                    && end.ids == expectedEndProps
                                    && setConstraints == expectedSetConstraints
                                    && key is String
                                    && commonRelAssertions(value)

                            oneProp -> start.ids == expectedPropsFirstKeyStrategy
                                    && end.ids == expectedEndProps
                                    && setConstraints == expectedSetConstraints
                                    && key is String
                                    && commonRelAssertions(value)

                            defaultProp -> start.ids == expectedPropsFirstKeyStrategy
                                    && end.ids == expectedEndProps
                                    && setConstraints == expectedSetConstraints
                                    && key is String
                                    && commonRelAssertions(value)

                            else -> false
                        }
                    }
                    else -> false
                }
            })

            db.execute("MATCH (p)-[rel:$allProps]->(pp) SET rel.type = 'update'").close()
            val updatedRecordsAll = consumer.poll(10000)
            assertEquals(1, updatedRecordsAll.count())
            val updatedAll = updatedRecordsAll.first()
            val keyUpdateAll = JSONUtils.readValue<Any>(updatedAll.key())
            val valueUpdateAll = JSONUtils.asStreamsTransactionEvent(updatedRecordsAll.first().value())
            val payloadUpdateAll = valueUpdateAll.payload as RelationshipPayload
            val (startUpdateAll, endUpdateAll, setConstraintsUpdateAll) = Triple(payloadUpdateAll.start, payloadUpdateAll.end, valueUpdateAll.schema.constraints.toSet())

            assertTrue {
                startUpdateAll.ids == mapOf("name" to "Foo", "surname" to "Bar")
                        && endUpdateAll.ids == mapOf("name" to "One")
                        && setConstraintsUpdateAll == setConstraintsUpdateAll
                        && keyUpdateAll is String
                        && commonRelAssertionsUpdate(valueUpdateAll)
            }

            db.execute("MATCH (p)-[rel:$oneProp]->(pp) SET rel.type = 'update'").close()
            val updatedRecordsOne = consumer.poll(10000)
            assertEquals(1, updatedRecordsOne.count())
            val updatedOne = updatedRecordsOne.first()
            val keyUpdateOne = JSONUtils.readValue<Any>(updatedOne.key())
            val valueUpdateOne = JSONUtils.asStreamsTransactionEvent(updatedRecordsOne.first().value())
            val payloadUpdateOne = valueUpdateOne.payload as RelationshipPayload
            val (startUpdateOne, endUpdateOne, setConstraintsUpdateOne) = Triple(payloadUpdateOne.start, payloadUpdateOne.end, valueUpdateOne.schema.constraints.toSet())

            assertTrue {
                startUpdateOne.ids == expectedPropsFirstKeyStrategy
                        && endUpdateOne.ids == expectedEndProps
                        && setConstraintsUpdateOne == setConstraintsUpdateAll
                        && keyUpdateOne is String
                        && commonRelAssertionsUpdate(valueUpdateOne)
            }

            db.execute("MATCH (p)-[rel:$defaultProp]->(pp) SET rel.type = 'update'").close()
            Thread.sleep(30000)
            val updatedRecords = consumer.poll(20000)
            assertEquals(1, updatedRecords.count())

            val updated = updatedRecords.first()
            val keyUpdate = JSONUtils.readValue<Any>(updated.key())
            val valueUpdate = JSONUtils.asStreamsTransactionEvent(updatedRecords.first().value())
            val payloadUpdate = valueUpdate.payload as RelationshipPayload
            val (startUpdate, endUpdate, setConstraintsUpdate) = Triple(payloadUpdate.start, payloadUpdate.end, valueUpdate.schema.constraints.toSet())

            assertTrue {
                startUpdate.ids == expectedPropsFirstKeyStrategy
                        && endUpdate.ids == expectedEndProps
                        && setConstraintsUpdate == setConstraintsUpdate
                        && keyUpdate is String
                        && commonRelAssertionsUpdate(valueUpdate)
            }

            db.execute("MATCH (p)-[rel:$allProps]->(pp) DELETE rel")

            val deletedRecordsAll = consumer.poll(10000)
            assertEquals(1, deletedRecordsAll.count())
            val recordAll = deletedRecordsAll.first()
            val keyAll = JSONUtils.readValue<Any>(recordAll.key())
            val valueAll = JSONUtils.asStreamsTransactionEvent(deletedRecordsAll.first().value())
            val payloadAll = valueAll.payload as RelationshipPayload
            val (startAll, endAll, setConstraintsAll) = Triple(payloadAll.start, payloadAll.end, valueAll.schema.constraints.toSet())
            assertTrue {
                startAll.ids == expectedPropsAllKeyStrategy
                        && endAll.ids == expectedEndProps
                        && setConstraintsAll == expectedSetConstraints
                        && keyAll is String
                        && commonRelAssertionsDelete(valueAll)
            }

            db.execute("MATCH (p)-[rel:$oneProp]->(pp) DELETE rel")

            val deletedRecordsOne = consumer.poll(10000)
            assertEquals(1, deletedRecordsOne.count())
            val recordOne = deletedRecordsOne.first()
            val keyOne = JSONUtils.readValue<Any>(recordOne.key())
            val valueOne = JSONUtils.asStreamsTransactionEvent(deletedRecordsOne.first().value())
            val payloadOne = valueOne.payload as RelationshipPayload
            val (startOne, endOne, setConstraintsOne) = Triple(payloadOne.start, payloadOne.end, valueOne.schema.constraints.toSet())
            assertTrue {
                startOne.ids == expectedPropsFirstKeyStrategy
                        && endOne.ids == expectedEndProps
                        && setConstraintsOne == expectedSetConstraints
                        && keyOne is String
                        && commonRelAssertionsDelete(valueOne)
            }

            db.execute("MATCH (p)-[rel:$defaultProp]->(pp) DELETE rel")

            val deletedRecords = consumer.poll(10000)
            assertEquals(1, deletedRecords.count())

            val record = deletedRecords.first()
            val key = JSONUtils.readValue<Any>(record.key())
            val value = JSONUtils.asStreamsTransactionEvent(deletedRecords.first().value())
            val payload = value.payload as RelationshipPayload
            val (start, end, setConstraints) = Triple(payload.start, payload.end, value.schema.constraints.toSet())
            assertTrue {
                start.ids == expectedPropsFirstKeyStrategy
                        && end.ids == expectedEndProps
                        && setConstraints == setConstraints
                        && key is String
                        && commonRelAssertionsDelete(value)
            }
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