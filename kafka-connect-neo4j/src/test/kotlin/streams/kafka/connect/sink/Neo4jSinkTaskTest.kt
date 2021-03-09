package streams.kafka.connect.sink

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.Timestamp
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask
import org.apache.kafka.connect.sink.SinkTaskContext
import org.junit.After
import org.junit.Before
import org.junit.Ignore
import org.junit.Test
import org.mockito.Mockito.mock
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.harness.ServerControls
import org.neo4j.harness.TestServerBuilders
import streams.events.*
import streams.serialization.JSONUtils
import streams.service.errors.ErrorService
import streams.service.sink.strategy.CUDNode
import streams.service.sink.strategy.CUDNodeRel
import streams.service.sink.strategy.CUDOperations
import streams.service.sink.strategy.CUDRelationship
import java.util.*
import java.util.stream.Collectors
import java.util.stream.StreamSupport
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue


class Neo4jSinkTaskTest {

    private lateinit var db: ServerControls

    @Before
    fun setUp() {
        db = TestServerBuilders.newInProcessBuilder()
                .withConfig("dbms.security.auth_enabled", "false")
                .newServer()
    }

    @After
    fun tearDown() {
        db.close()
    }

    private val PERSON_SCHEMA = SchemaBuilder.struct().name("com.example.Person")
            .field("firstName", Schema.STRING_SCHEMA)
            .field("lastName", Schema.STRING_SCHEMA)
            .field("age", Schema.OPTIONAL_INT32_SCHEMA)
            .field("bool", Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field("short", Schema.OPTIONAL_INT16_SCHEMA)
            .field("byte", Schema.OPTIONAL_INT8_SCHEMA)
            .field("long", Schema.OPTIONAL_INT64_SCHEMA)
            .field("float", Schema.OPTIONAL_FLOAT32_SCHEMA)
            .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field("modified", Timestamp.SCHEMA)
            .build()


    private val PLACE_SCHEMA = SchemaBuilder.struct().name("com.example.Place")
            .field("name", Schema.STRING_SCHEMA)
            .field("latitude", Schema.FLOAT32_SCHEMA)
            .field("longitude", Schema.FLOAT32_SCHEMA)
            .build()

    private val PERSON_SCHEMA_EXT = SchemaBuilder.struct().name("com.example.Person")
            .field("firstName", Schema.STRING_SCHEMA)
            .field("lastName", Schema.STRING_SCHEMA)
            .field("age", Schema.OPTIONAL_INT32_SCHEMA)
            .field("bool", Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field("short", Schema.OPTIONAL_INT16_SCHEMA)
            .field("byte", Schema.OPTIONAL_INT8_SCHEMA)
            .field("long", Schema.OPTIONAL_INT64_SCHEMA)
            .field("float", Schema.OPTIONAL_FLOAT32_SCHEMA)
            .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field("modified", Timestamp.SCHEMA)
            .field("visited", SchemaBuilder.array(PLACE_SCHEMA))
            .build()



    @Test
    fun `test array of struct`() {
        val firstTopic = "neotopic"
        val props = mutableMapOf<String, String>()
        props[Neo4jSinkConnectorConfig.SERVER_URI] = db.boltURI().toString()
        props["${Neo4jSinkConnectorConfig.TOPIC_CYPHER_PREFIX}$firstTopic"] = """
            CREATE (b:BODY)
            WITH event.p AS paragraphList, event.ul AS ulList, b
            FOREACH (paragraph IN paragraphList | CREATE (b)-[:HAS_P]->(p:P{value: paragraph.value}))

            WITH ulList, b
            UNWIND ulList AS ulElem
            CREATE (b)-[:HAS_UL]->(ul:UL)

            WITH ulElem, ul
            UNWIND ulElem.value AS liElem
            CREATE (ul)-[:HAS_LI]->(li:LI{value: liElem.value, class: liElem.class})
        """.trimIndent()
        props[Neo4jSinkConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()
        props[Neo4jSinkConnectorConfig.BATCH_SIZE] = 2.toString()
        props[SinkTask.TOPICS_CONFIG] = "$firstTopic"

        val task = Neo4jSinkTask()
        task.initialize(mock(SinkTaskContext::class.java))
        task.start(props)
        val input = listOf(SinkRecord(firstTopic, 1, null, null, PERSON_SCHEMA, Neo4jValueConverterTest.getTreeStruct(), 42))
        task.put(input)
        db.graph().beginTx().use {
            assertEquals(1, db.graph().findNodes(Label.label("BODY")).stream().count())
            assertEquals(2, db.graph().findNodes(Label.label("P")).stream().count())
            assertEquals(2, db.graph().findNodes(Label.label("UL")).stream().count())
            assertEquals(4, db.graph().findNodes(Label.label("LI")).stream().count())

            assertEquals(2, db.graph().execute("MATCH (b:BODY)-[r:HAS_P]->(p:P) RETURN COUNT(r) AS COUNT").columnAs<Long>("COUNT").next())
            assertEquals(2, db.graph().execute("MATCH (b:BODY)-[r:HAS_UL]->(ul:UL) RETURN COUNT(r) AS COUNT").columnAs<Long>("COUNT").next())
            assertEquals(4, db.graph().execute("MATCH (ul:UL)-[r:HAS_LI]->(li:LI) RETURN COUNT(r) AS COUNT").columnAs<Long>("COUNT").next())

            assertEquals(1, db.graph().execute("MATCH (li:LI{class:['ClassA', 'ClassB']}) RETURN COUNT(li) AS COUNT").columnAs<Long>("COUNT").next())
        }
    }


    @Test
    fun `should insert data into Neo4j`() {
        val firstTopic = "neotopic"
        val secondTopic = "foo"
        val props = mutableMapOf<String, String>()
        props[Neo4jSinkConnectorConfig.SERVER_URI] = db.boltURI().toString()
        props["${Neo4jSinkConnectorConfig.TOPIC_CYPHER_PREFIX}$firstTopic"] = "CREATE (n:PersonExt {name: event.firstName, surname: event.lastName})"
        props["${Neo4jSinkConnectorConfig.TOPIC_CYPHER_PREFIX}$secondTopic"] = "CREATE (n:Person {name: event.firstName})"
        props[Neo4jSinkConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()
        props[Neo4jSinkConnectorConfig.BATCH_SIZE] = 2.toString()
        props[SinkTask.TOPICS_CONFIG] = "$firstTopic,$secondTopic"

        val struct= Struct(PERSON_SCHEMA)
                .put("firstName", "Alex")
                .put("lastName", "Smith")
                .put("bool", true)
                .put("short", 1234.toShort())
                .put("byte", (-32).toByte())
                .put("long", 12425436L)
                .put("float", 2356.3.toFloat())
                .put("double", -2436546.56457)
                .put("age", 21)
                .put("modified", Date(1474661402123L))

        val task = Neo4jSinkTask()
        task.initialize(mock(SinkTaskContext::class.java))
        task.start(props)
        val input = listOf(SinkRecord(firstTopic, 1, null, null, PERSON_SCHEMA, struct, 42),
                SinkRecord(firstTopic, 1, null, null, PERSON_SCHEMA, struct, 42),
                SinkRecord(firstTopic, 1, null, null, PERSON_SCHEMA, struct, 42),
                SinkRecord(firstTopic, 1, null, null, PERSON_SCHEMA, struct, 42),
                SinkRecord(firstTopic, 1, null, null, PERSON_SCHEMA, struct, 42),
                SinkRecord(secondTopic, 1, null, null, PERSON_SCHEMA, struct, 43))
        task.put(input)
        db.graph().beginTx().use {
            val personCount = db.graph().execute("MATCH (p:Person) RETURN COUNT(p) as COUNT").columnAs<Long>("COUNT").next()
            val expectedPersonCount = input.filter { it.topic() == secondTopic }.size
            assertEquals(expectedPersonCount, personCount.toInt())
            val personExtCount = db.graph().execute("MATCH (p:PersonExt) RETURN COUNT(p) as COUNT").columnAs<Long>("COUNT").next()
            val expectedPersonExtCount = input.filter { it.topic() == firstTopic }.size
            assertEquals(expectedPersonExtCount, personExtCount.toInt())
        }
    }

    @Test
    fun `should insert data into Neo4j from CDC events`() {
        val firstTopic = "neotopic"
        val props = mapOf(Neo4jSinkConnectorConfig.SERVER_URI to db.boltURI().toString(),
                Neo4jSinkConnectorConfig.TOPIC_CDC_SOURCE_ID to firstTopic,
                Neo4jSinkConnectorConfig.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
                SinkTask.TOPICS_CONFIG to firstTopic)

        val cdcDataStart = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
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
        val cdcDataEnd = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
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
        val cdcDataRelationship = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                username = "user",
                txId = 1,
                txEventId = 2,
                txEventsCount = 3,
                operation = OperationType.created
            ),
            payload = RelationshipPayload(
                id = "2",
                start = RelationshipNodeChange(id = "0", labels = listOf("User"), ids = emptyMap()),
                end = RelationshipNodeChange(id = "1", labels = listOf("User Ext"), ids = emptyMap()),
                after = RelationshipChange(properties = mapOf("since" to 2014)),
                before = null,
                label = "KNOWS WHO"
            ),
            schema = Schema()
        )

        val task = Neo4jSinkTask()
        task.initialize(mock(SinkTaskContext::class.java))
        task.start(props)
        val input = listOf(SinkRecord(firstTopic, 1, null, null, null, cdcDataStart, 42),
                SinkRecord(firstTopic, 1, null, null, null, cdcDataEnd, 43),
                SinkRecord(firstTopic, 1, null, null, null, cdcDataRelationship, 44))
        task.put(input)
        db.graph().beginTx().use {
            db.graph().execute("""
                MATCH p = (s:User{name:'Andrea', `comp@ny`:'LARUS-BA', sourceId:'0'})
                    -[r:`KNOWS WHO`{since:2014, sourceId:'2'}]->
                    (e:`User Ext`{name:'Michael', `comp@ny`:'Neo4j', sourceId:'1'})
                RETURN count(p) AS count
            """.trimIndent())
                    .columnAs<Long>("count").use {
                        assertTrue { it.hasNext() }
                        val count = it.next()
                        assertEquals(1, count)
                        assertFalse { it.hasNext() }
                    }
        }
    }

    @Test
    fun `should update data into Neo4j from CDC events`() {
        db.graph().beginTx().use {
            db.graph().execute("""
                CREATE (s:User:OldLabel:SourceEvent{name:'Andrea', `comp@ny`:'LARUS-BA', sourceId:'0'})
                    -[r:`KNOWS WHO`{since:2014, sourceId:'2'}]->
                    (e:`User Ext`:SourceEvent{name:'Michael', `comp@ny`:'Neo4j', sourceId:'1'})
            """.trimIndent()).close()
            it.success()
        }
        val firstTopic = "neotopic"
        val props = mapOf(Neo4jSinkConnectorConfig.SERVER_URI to db.boltURI().toString(),
                Neo4jSinkConnectorConfig.TOPIC_CDC_SOURCE_ID to firstTopic,
                Neo4jSinkConnectorConfig.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
                SinkTask.TOPICS_CONFIG to firstTopic)

        val cdcDataStart = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                username = "user",
                txId = 1,
                txEventId = 0,
                txEventsCount = 3,
                operation = OperationType.updated
            ),
            payload = NodePayload(id = "0",
                before = NodeChange(properties = mapOf("name" to "Andrea", "comp@ny" to "LARUS-BA"),
                    labels = listOf("User", "OldLabel")),
                after = NodeChange(properties = mapOf("name" to "Andrea", "comp@ny" to "LARUS-BA, Venice", "age" to 34),
                    labels = listOf("User"))
            ),
            schema = Schema()
        )
        val cdcDataRelationship = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                username = "user",
                txId = 1,
                txEventId = 2,
                txEventsCount = 3,
                operation = OperationType.updated
            ),
            payload = RelationshipPayload(
                id = "2",
                start = RelationshipNodeChange(id = "0", labels = listOf("User"), ids = emptyMap()),
                end = RelationshipNodeChange(id = "1", labels = listOf("User Ext"), ids = emptyMap()),
                after = RelationshipChange(properties = mapOf("since" to 2014, "foo" to "bar")),
                before = RelationshipChange(properties = mapOf("since" to 2014)),
                label = "KNOWS WHO"
            ),
            schema = Schema()
        )

        val task = Neo4jSinkTask()
        task.initialize(mock(SinkTaskContext::class.java))
        task.start(props)
        val input = listOf(SinkRecord(firstTopic, 1, null, null, null, cdcDataStart, 42),
                SinkRecord(firstTopic, 1, null, null, null, cdcDataRelationship, 43))
        task.put(input)
        db.graph().beginTx().use {
            db.graph().execute("""
                MATCH p = (s:User{name:'Andrea', `comp@ny`:'LARUS-BA, Venice', sourceId:'0', age:34})
                    -[r:`KNOWS WHO`{since:2014, sourceId:'2', foo:'bar'}]->
                    (e:`User Ext`{name:'Michael', `comp@ny`:'Neo4j', sourceId:'1'})
                RETURN count(p) AS count
            """.trimIndent())
                    .columnAs<Long>("count").use {
                        assertTrue { it.hasNext() }
                        val count = it.next()
                        assertEquals(1, count)
                        assertFalse { it.hasNext() }
                    }
        }
    }

    @Test
    fun `should insert data into Neo4j from CDC events with schema strategy`() {
        val firstTopic = "neotopic"
        val props = mapOf(Neo4jSinkConnectorConfig.SERVER_URI to db.boltURI().toString(),
                Neo4jSinkConnectorConfig.TOPIC_CDC_SCHEMA to firstTopic,
                Neo4jSinkConnectorConfig.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
                SinkTask.TOPICS_CONFIG to firstTopic)

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

        val task = Neo4jSinkTask()
        task.initialize(mock(SinkTaskContext::class.java))
        task.start(props)
        val input = listOf(SinkRecord(firstTopic, 1, null, null, null, cdcDataStart, 42),
                SinkRecord(firstTopic, 1, null, null, null, cdcDataEnd, 43),
                SinkRecord(firstTopic, 1, null, null, null, cdcDataRelationship, 44))
        task.put(input)
        db.graph().beginTx().use {
            val query = """
                |MATCH p = (s:User{name:'Andrea', surname:'Santurbano', `comp@ny`:'LARUS-BA'})-[r:`KNOWS WHO`{since:2014}]->(e:User{name:'Michael', surname:'Hunger', `comp@ny`:'Neo4j'})
                |RETURN count(p) AS count
                |""".trimMargin()
            db.graph().execute(query)
                    .columnAs<Long>("count").use {
                        assertTrue { it.hasNext() }
                        val count = it.next()
                        assertEquals(1, count)
                        assertFalse { it.hasNext() }
                    }

            val labels = db.graph().allLabels.stream().map { it.name() }.collect(Collectors.toSet())
            assertEquals(setOf("User"), labels)
        }
    }

    @Test
    fun `should insert data into Neo4j from CDC events with schema strategy, multiple constraints and labels`() {
        val myTopic = UUID.randomUUID().toString()
        val props = mapOf(Neo4jSinkConnectorConfig.SERVER_URI to db.boltURI().toString(),
                Neo4jSinkConnectorConfig.TOPIC_CDC_SCHEMA to myTopic,
                Neo4jSinkConnectorConfig.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
                SinkTask.TOPICS_CONFIG to myTopic)

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
                        after = NodeChange(properties = mapOf("firstName" to "Minato", "lastName" to "Namikaze"), labels = listOf("Writer"))
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
                        // leverage on first ids alphabetically, that is name, so we take the 2 previously created nodes
                        start = RelationshipNodeChange(id = "99", labels = listOf("Character"), ids = mapOf("name" to "Naruto", "surname" to "Osvaldo", "address" to "Land of Sand")),
                        end = RelationshipNodeChange(id = "88", labels = listOf("Writer"), ids = mapOf("firstName" to "Minato", "lastName" to "Franco", "address" to "Land of Fire")),
                        after = RelationshipChange(properties = mapOf("since" to 2014)),
                        before = null,
                        label = "KNOWS WHO"
                ),
                schema = relSchema
        )

        val task = Neo4jSinkTask()
        task.initialize(mock(SinkTaskContext::class.java))
        task.start(props)
        val input = listOf(SinkRecord(myTopic, 1, null, null, null, cdcDataStart, 42),
                SinkRecord(myTopic, 1, null, null, null, cdcDataEnd, 43),
                SinkRecord(myTopic, 1, null, null, null, cdcDataRelationship, 44))

        task.put(input)

        db.graph().beginTx().use {
            val query = """
                |MATCH p = (s:Character)-[r:`KNOWS WHO`{since: 2014}]->(e:Writer)
                |RETURN count(p) AS count
                |""".trimMargin()
            db.graph().execute(query)
                    .columnAs<Long>("count").use {
                        assertTrue { it.hasNext() }
                        val path = it.next()
                        assertEquals(1, path)
                        assertFalse { it.hasNext() }
                    }

            val countAllNodes = db.graph().beginTx().use { db.graph().allNodes.stream().count() }
            assertEquals(2L, countAllNodes)
        }

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
                        after = RelationshipChange(properties = mapOf("since" to 1998)),
                        before = null,
                        label = "HAS WRITTEN"
                ),
                schema = relSchema
        )

        val inputNotMatched = listOf(SinkRecord(myTopic, 1, null, null, null, cdcDataRelationshipNotMatched, 45))

        task.put(inputNotMatched)

        // todo - mettere in variabile?
        val graph = db.graph()
        graph.beginTx().use {
            val query = """
                |MATCH p = (s:Character)-[r:`HAS WRITTEN`{since: 1998}]->(e:Writer)
                |RETURN count(p) AS count
                |""".trimMargin()
            graph.execute(query)
                    .columnAs<Long>("count").use {
                        assertTrue { it.hasNext() }
                        val path = it.next()
                        assertEquals(1, path)
                        assertFalse { it.hasNext() }
                    }

            val countAllNodes = db.graph().beginTx().use { db.graph().allNodes.stream().count() }
            assertEquals(4L, countAllNodes)
        }

    }

    @Test
    fun `should insert data into Neo4j from CDC events with schema strategy and multiple unique constraints merging previous nodes`() {
        val myTopic = UUID.randomUUID().toString()
        val props = mapOf(Neo4jSinkConnectorConfig.SERVER_URI to db.boltURI().toString(),
                Neo4jSinkConnectorConfig.TOPIC_CDC_SCHEMA to myTopic,
                Neo4jSinkConnectorConfig.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
                SinkTask.TOPICS_CONFIG to myTopic)

        val constraints = listOf(
                Constraint(label = "User", type = StreamsConstraintType.UNIQUE, properties = setOf("name")),
                Constraint(label = "User", type = StreamsConstraintType.UNIQUE, properties = setOf("country", "address")),
                Constraint(label = "User", type = StreamsConstraintType.UNIQUE, properties = setOf("surname"))
        )
        val relSchema = Schema(properties = mapOf("since" to "Long"), constraints = constraints)
        val nodeSchema = Schema(properties = mapOf("name" to "String", "surname" to "String", "country" to "String", "address" to "String"),
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

        val task = Neo4jSinkTask()
        task.initialize(mock(SinkTaskContext::class.java))
        task.start(props)
        val input = listOf(SinkRecord(myTopic, 1, null, null, null, cdcDataStart, 42),
                SinkRecord(myTopic, 1, null, null, null, cdcDataEnd, 43),
                SinkRecord(myTopic, 1, null, null, null, cdcDataRelationship, 44))

        task.put(input)

        db.graph().beginTx().use {
            val query = """
                |MATCH p = (s:User)-[r:`KNOWS WHO` {since: 2014}]->(e:User)
                |RETURN count(p) as count
                |""".trimMargin()
            db.graph().execute(query)
                    .columnAs<Long>("count").use {
                        assertTrue { it.hasNext() }
                        val path = it.next()
                        assertEquals(1, path)
                        assertFalse { it.hasNext() }
                    }

            val labels = db.graph().beginTx()
                    .use { StreamSupport.stream(db.graph().allLabels.spliterator(), false).map { it.name() }.collect(Collectors.toSet()) }
            assertEquals(setOf("User"), labels)

            val countUsers = db.graph().beginTx().use { db.graph().findNodes(Label.label("User")).stream().count() }
            assertEquals(2L, countUsers)
        }


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
                        label = "HAS WRITTEN"
                ),
                schema = relSchema
        )

        val inputNotMatched = listOf(SinkRecord(myTopic, 1, null, null, null, cdcDataRelationshipNotMatched, 45))

        task.put(inputNotMatched)

        db.graph().beginTx().use {
            val query = """
                |MATCH p = (s:User)-[r:`HAS WRITTEN`{since: 2000}]->(e:User)
                |RETURN count(p) AS count
                |""".trimMargin()
            db.graph().execute(query)
                    .columnAs<Long>("count").use {
                        assertTrue { it.hasNext() }
                        val path = it.next()
                        assertEquals(1, path)
                        assertFalse { it.hasNext() }
                    }

            val labels = db.graph().beginTx()
                    .use { StreamSupport.stream(db.graph().allLabels.spliterator(), false).map { it.name() }.collect(Collectors.toSet()) }
            assertEquals(setOf("User"), labels)

            val countUsers = db.graph().beginTx().use { db.graph().allNodes.stream().count() }
            assertEquals(4L, countUsers)
        }
    }

    @Test
    fun `should insert data into Neo4j from CDC events with schema strategy and multiple unique constraints`() {
        val myTopic = UUID.randomUUID().toString()
        val props = mapOf(Neo4jSinkConnectorConfig.SERVER_URI to db.boltURI().toString(),
                Neo4jSinkConnectorConfig.TOPIC_CDC_SCHEMA to myTopic,
                Neo4jSinkConnectorConfig.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
                SinkTask.TOPICS_CONFIG to myTopic)

        val constraints = listOf(
                Constraint(label = "User", type = StreamsConstraintType.UNIQUE, properties = setOf("name")),
                Constraint(label = "User", type = StreamsConstraintType.UNIQUE, properties = setOf("surname")),
                Constraint(label = "User", type = StreamsConstraintType.UNIQUE, properties = setOf("country", "address"))
        )
        val relSchema = Schema(properties = mapOf("since" to "Long"), constraints = constraints)
        val nodeSchema = Schema(properties = mapOf("name" to "String", "surname" to "String", "country" to "String", "address" to "String"),
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
                        // leverage on first ids alphabetically, that is name, so create 2 additional nodes
                        start = RelationshipNodeChange(id = "1", labels = listOf("User"), ids = mapOf("name" to "Invalid", "surname" to "Uzumaki")),
                        end = RelationshipNodeChange(id = "2", labels = listOf("User"), ids = mapOf("name" to "AnotherInvalid", "surname" to "Namikaze")),
                        after = RelationshipChange(properties = mapOf("since" to 2014)),
                        before = null,
                        label = "KNOWS WHO"
                ),
                schema = relSchema
        )

        val task = Neo4jSinkTask()
        task.initialize(mock(SinkTaskContext::class.java))
        task.start(props)
        val input = listOf(
                SinkRecord(myTopic, 1, null, null, null, cdcDataStart, 42),
                SinkRecord(myTopic, 1, null, null, null, cdcDataEnd, 43),
                SinkRecord(myTopic, 1, null, null, null, cdcDataRelationship, 44)
        )
        task.put(input)

        db.graph().beginTx().use {
            val query = """
                |MATCH p = (s:User)-[r:`KNOWS WHO` {since: 2014}]->(e:User)
                |RETURN count(p) as count
                |""".trimMargin()
            db.graph().execute(query)
                    .columnAs<Long>("count").use {
                        assertTrue { it.hasNext() }
                        val path = it.next()
                        assertEquals(1, path)
                        assertFalse { it.hasNext() }
                    }

            val labels = db.graph().beginTx()
                    .use { StreamSupport.stream(db.graph().allLabels.spliterator(), false).map { it.name() }.collect(Collectors.toSet()) }
            assertEquals(setOf("User"), labels)

            val countUsers = db.graph().beginTx().use { db.graph().findNodes(Label.label("User")).stream().count() }
            assertEquals(4L, countUsers)
        }
    }


    @Test
    fun `should delete data into Neo4j from CDC events`() {
        db.graph().beginTx().use {
            db.graph().execute("""
                CREATE (s:User:OldLabel:SourceEvent{name:'Andrea', `comp@ny`:'LARUS-BA', sourceId:'0'})
                    -[r:`KNOWS WHO`{since:2014, sourceId:'2'}]->
                    (e:`User Ext`:SourceEvent{name:'Michael', `comp@ny`:'Neo4j', sourceId:'1'})
            """.trimIndent()).close()
            it.success()
        }
        val firstTopic = "neotopic"
        val props = mapOf(Neo4jSinkConnectorConfig.SERVER_URI to db.boltURI().toString(),
                Neo4jSinkConnectorConfig.TOPIC_CDC_SOURCE_ID to firstTopic,
                Neo4jSinkConnectorConfig.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
                SinkTask.TOPICS_CONFIG to firstTopic)

        val cdcDataStart = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                username = "user",
                txId = 1,
                txEventId = 0,
                txEventsCount = 3,
                operation = OperationType.deleted
            ),
            payload = NodePayload(id = "0",
                before = NodeChange(properties = mapOf("name" to "Andrea", "comp@ny" to "LARUS-BA"),
                    labels = listOf("User", "OldLabel")),
                after = null
            ),
            schema = Schema()
        )
        val task = Neo4jSinkTask()
        task.initialize(mock(SinkTaskContext::class.java))
        task.start(props)
        val input = listOf(SinkRecord(firstTopic, 1, null, null, null, cdcDataStart, 42))
        task.put(input)
        db.graph().beginTx().use {
            db.graph().execute("""
                    MATCH (s:SourceEvent)
                    RETURN count(s) as count
                """.trimIndent())
                    .columnAs<Long>("count").use {
                        assertTrue { it.hasNext() }
                        val count = it.next()
                        assertEquals(1, count)
                        assertFalse { it.hasNext() }
                    }
        }
    }

    @Test
    fun `should not insert data into Neo4j`() {
        val topic = "neotopic"
        val props = mutableMapOf<String, String>()
        props[Neo4jSinkConnectorConfig.SERVER_URI] = db.boltURI().toString()
        props["${Neo4jSinkConnectorConfig.TOPIC_CYPHER_PREFIX}$topic"] = "CREATE (n:Person {name: event.firstName, surname: event.lastName})"
        props[Neo4jSinkConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()
        props[SinkTask.TOPICS_CONFIG] = topic

        val struct= Struct(PLACE_SCHEMA)
                .put("name", "San Mateo (CA)")
                .put("latitude", 37.5629917.toFloat())
                .put("longitude", -122.3255254.toFloat())

        val task = Neo4jSinkTask()
        task.initialize(mock(SinkTaskContext::class.java))
        task.start(props)
        task.put(listOf(SinkRecord(topic, 1, null, null, PERSON_SCHEMA, struct, 42)))
        db.graph().beginTx().use {
            val node: Node? = db.graph().findNode(Label.label("Person"), "name", "Alex")
            assertTrue { node == null }
        }
    }

    @Test
    fun `should report but not fail parsing data`() {
        val topic = "neotopic"
        val props = mutableMapOf<String, String>()
        props[Neo4jSinkConnectorConfig.SERVER_URI] = db.boltURI().toString()
        props["${Neo4jSinkConnectorConfig.TOPIC_CYPHER_PREFIX}$topic"] = "CREATE (n:Person {name: event.firstName, surname: event.lastName})"
        props[Neo4jSinkConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()
        props[SinkTask.TOPICS_CONFIG] = topic
        props[ErrorService.ErrorConfig.TOLERANCE] = "all"
        props[ErrorService.ErrorConfig.LOG] = true.toString()

        val task = Neo4jSinkTask()
        task.initialize(mock(SinkTaskContext::class.java))
        task.start(props)
        task.put(listOf(SinkRecord(topic, 1, null, null, null, "a", 42)))
        db.graph().beginTx().use {
            val node: Node? = db.graph().findNode(Label.label("Person"), "name", "Alex")
            assertTrue { node == null }
        }
    }

    @Test
    fun `should report but not fail invalid schema`() {
        val topic = "neotopic"
        val props = mutableMapOf<String, String>()
        props[Neo4jSinkConnectorConfig.SERVER_URI] = db.boltURI().toString()
        props["${Neo4jSinkConnectorConfig.TOPIC_CYPHER_PREFIX}$topic"] = "CREATE (n:Person {name: event.firstName, surname: event.lastName})"
        props[Neo4jSinkConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()
        props[ErrorService.ErrorConfig.TOLERANCE] = "all"
        props[ErrorService.ErrorConfig.LOG] = true.toString()
        props[SinkTask.TOPICS_CONFIG] = topic

        val task = Neo4jSinkTask()
        task.initialize(mock(SinkTaskContext::class.java))
        task.start(props)
        task.put(listOf(SinkRecord(topic, 1, null, 42, null, "true", 42)))
        db.graph().beginTx().use {
            val node: Node? = db.graph().findNode(Label.label("Person"), "name", "Alex")
            assertTrue { node == null }
        }
    }

    @Test
    fun `should fail running invalid cypher`() {
        val topic = "neotopic"
        val props = mutableMapOf<String, String>()
        props[Neo4jSinkConnectorConfig.SERVER_URI] = db.boltURI().toString()
        props["${Neo4jSinkConnectorConfig.TOPIC_CYPHER_PREFIX}$topic"] = " No Valid Cypher "
        props[Neo4jSinkConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()
        props[SinkTask.TOPICS_CONFIG] = topic
        props[ErrorService.ErrorConfig.TOLERANCE] = "all"
        props[ErrorService.ErrorConfig.LOG] = true.toString()

        val task = Neo4jSinkTask()
        task.initialize(mock(SinkTaskContext::class.java))
        task.start(props)
        task.put(listOf(SinkRecord(topic, 1, null, 42, null, "{\"foo\":42}", 42)))
        db.graph().beginTx().use {
            val node: Node? = db.graph().findNode(Label.label("Person"), "name", "Alex")
            assertTrue { node == null }
        }
    }

    @Test
    fun `should work with node pattern topic`() {
        val topic = "neotopic"
        val props = mutableMapOf<String, String>()
        props[Neo4jSinkConnectorConfig.SERVER_URI] = db.boltURI().toString()
        props["${Neo4jSinkConnectorConfig.TOPIC_PATTERN_NODE_PREFIX}$topic"] = "User{!userId,name,surname,address.city}"
        props[Neo4jSinkConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()
        props[SinkTask.TOPICS_CONFIG] = topic

        val data = mapOf("userId" to 1, "name" to "Andrea", "surname" to "Santurbano",
                "address" to mapOf("city" to "Venice", "CAP" to "30100"))

        val task = Neo4jSinkTask()
        task.initialize(mock(SinkTaskContext::class.java))
        task.start(props)
        val input = listOf(SinkRecord(topic, 1, null, null, null, data, 42))
        task.put(input)
        db.graph().beginTx().use {
            db.graph().execute("MATCH (n:User{name: 'Andrea', surname: 'Santurbano', userId: 1, `address.city`: 'Venice'}) RETURN count(n) AS count")
                    .columnAs<Long>("count").use {
                        assertTrue { it.hasNext() }
                        val count = it.next()
                        assertEquals(1, count)
                        assertFalse { it.hasNext() }
                    }
        }
    }

    @Test
    fun `should work with relationship pattern topic`() {
        val topic = "neotopic"
        val props = mutableMapOf<String, String>()
        props[Neo4jSinkConnectorConfig.SERVER_URI] = db.boltURI().toString()
        props["${Neo4jSinkConnectorConfig.TOPIC_PATTERN_RELATIONSHIP_PREFIX}$topic"] = "(:User{!sourceId,sourceName,sourceSurname})-[:KNOWS]->(:User{!targetId,targetName,targetSurname})"
        props[Neo4jSinkConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()
        props[SinkTask.TOPICS_CONFIG] = topic

        val data = mapOf("sourceId" to 1, "sourceName" to "Andrea", "sourceSurname" to "Santurbano",
                "targetId" to 1, "targetName" to "Michael", "targetSurname" to "Hunger", "since" to 2014)

        val task = Neo4jSinkTask()
        task.initialize(mock(SinkTaskContext::class.java))
        task.start(props)
        val input = listOf(SinkRecord(topic, 1, null, null, null, data, 42))
        task.put(input)
        db.graph().beginTx().use {
            db.graph().execute("""
                MATCH p = (s:User{sourceName: 'Andrea', sourceSurname: 'Santurbano', sourceId: 1})-[:KNOWS{since: 2014}]->(e:User{targetName: 'Michael', targetSurname: 'Hunger', targetId: 1})
                RETURN count(p) AS count
            """.trimIndent())
                    .columnAs<Long>("count").use {
                        assertTrue { it.hasNext() }
                        val count = it.next()
                        assertEquals(1, count)
                        assertFalse { it.hasNext() }
                    }
        }
    }

    @Test
    fun `should work with node pattern topic for tombstone record`() {
        db.graph().beginTx().use {
            db.graph().execute("CREATE (u:User{userId: 1, name: 'Andrea', surname: 'Santurbano'})").close()
            it.success()
        }
        val count = db.graph().beginTx().use {
            db.graph().execute("MATCH (n) RETURN count(n) AS count")
                    .columnAs<Long>("count")
                    .next()
        }
        assertEquals(1L, count)
        val topic = "neotopic"
        val props = mutableMapOf<String, String>()
        props[Neo4jSinkConnectorConfig.SERVER_URI] = db.boltURI().toString()
        props["${Neo4jSinkConnectorConfig.TOPIC_PATTERN_NODE_PREFIX}$topic"] = "User{!userId,name,surname,address.city}"
        props[Neo4jSinkConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()
        props[SinkTask.TOPICS_CONFIG] = topic

        val data = mapOf("userId" to 1)

        val task = Neo4jSinkTask()
        task.initialize(mock(SinkTaskContext::class.java))
        task.start(props)
        val input = listOf(SinkRecord(topic, 1, null, data, null, null, 42))
        task.put(input)
        db.graph().beginTx().use {
            db.graph().execute("MATCH (n) RETURN count(n) AS count")
                    .columnAs<Long>("count").use {
                        assertTrue { it.hasNext() }
                        val count = it.next()
                        assertEquals(0, count)
                        assertFalse { it.hasNext() }
                    }
        }
    }

    @Test
    fun `should ingest node data from CUD Events`() {
        // given
        val mergeMarkers = listOf(2, 5, 7)
        val key = "key"
        val topic = UUID.randomUUID().toString()
        val data = (1..10).map {
            val labels = if (it % 2 == 0) listOf("Foo", "Bar") else listOf("Foo", "Bar", "Label")
            val properties = mapOf("foo" to "foo-value-$it", "id" to it)
            val (op, ids) = when (it) {
                in mergeMarkers -> CUDOperations.merge to mapOf(key to it)
                else -> CUDOperations.create to emptyMap()
            }
            val cudNode = CUDNode(op = op,
                    labels = labels,
                    ids = ids,
                    properties = properties)
            SinkRecord(topic, 1, null, null, null, JSONUtils.asMap(cudNode), it.toLong())
        }
        val props = mutableMapOf<String, String>()
        props[Neo4jSinkConnectorConfig.SERVER_URI] = db.boltURI().toString()
        props[Neo4jSinkConnectorConfig.TOPIC_CUD] = topic
        props[Neo4jSinkConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()
        props[SinkTask.TOPICS_CONFIG] = topic

        // when
        val task = Neo4jSinkTask()
        task.initialize(mock(SinkTaskContext::class.java))
        task.start(props)
        task.put(data)

        // then
        db.graph().beginTx().use {
            val countFooBarLabel = db.graph().execute("MATCH (n:Foo:Bar:Label) RETURN count(n) AS count")
                    .columnAs<Long>("count")
                    .next()
            assertEquals(5L, countFooBarLabel)
            val countFooBar = db.graph().execute("MATCH (n:Foo:Bar) RETURN count(n) AS count")
                    .columnAs<Long>("count")
                    .next()
            assertEquals(10L, countFooBar)
        }
    }

    @Test
    fun `should ingest relationship data from CUD Events`() {
        // given
        val key = "key"
        val topic = UUID.randomUUID().toString()
        val rel_type = "MY_REL"
        val data = (1..10).map {
            val properties = mapOf("foo" to "foo-value-$it", "id" to it)
            val start = CUDNodeRel(ids = mapOf(key to it), labels = listOf("Foo", "Bar"))
            val end = CUDNodeRel(ids = mapOf(key to it), labels = listOf("FooBar"))
            val rel = CUDRelationship(op = CUDOperations.create, properties = properties, from = start, to = end, rel_type = rel_type)
            SinkRecord(topic, 1, null, null, null, JSONUtils.asMap(rel), it.toLong())
        }
        val props = mutableMapOf<String, String>()
        props[Neo4jSinkConnectorConfig.SERVER_URI] = db.boltURI().toString()
        props[Neo4jSinkConnectorConfig.TOPIC_CUD] = topic
        props[Neo4jSinkConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()
        props[SinkTask.TOPICS_CONFIG] = topic
        db.graph().beginTx().use {
            db.graph().execute("""
                UNWIND range(1, 10) AS id
                CREATE (:Foo:Bar {key: id})
                CREATE (:FooBar {key: id})
            """.trimIndent()).close()
            assertEquals(0, db.graph().allRelationships.count())
            assertEquals(20, db.graph().allNodes.count())
            it.success()
        }

        // when
        val task = Neo4jSinkTask()
        task.initialize(mock(SinkTaskContext::class.java))
        task.start(props)
        task.put(data)

        // then
        db.graph().beginTx().use {
            val countFooBarLabel = db.graph().execute("""
                MATCH (:Foo:Bar)-[r:$rel_type]->(:FooBar)
                RETURN count(r) AS count
            """.trimIndent())
                    .columnAs<Long>("count").next()
            assertEquals(10L, countFooBarLabel)
        }
    }

}