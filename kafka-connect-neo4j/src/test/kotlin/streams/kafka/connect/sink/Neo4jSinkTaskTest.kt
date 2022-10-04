package streams.kafka.connect.sink

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.Timestamp
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask
import org.apache.kafka.connect.sink.SinkTaskContext
import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Ignore
import org.junit.Test
import org.mockito.Mockito.mock
import org.neo4j.driver.Driver
import org.neo4j.driver.Session
import org.neo4j.driver.types.Node
import org.neo4j.graphdb.Label
import streams.Neo4jContainerExtension
import streams.events.*
import streams.kafka.connect.common.Neo4jConnectorConfig
import streams.kafka.connect.utils.allLabels
import streams.kafka.connect.utils.allNodes
import streams.kafka.connect.utils.allRelationships
import streams.kafka.connect.utils.findNode
import streams.kafka.connect.utils.findNodes
import streams.service.errors.ErrorService
import streams.service.errors.ProcessingError
import streams.service.sink.strategy.CUDNode
import streams.service.sink.strategy.CUDNodeRel
import streams.service.sink.strategy.CUDOperations
import streams.service.sink.strategy.CUDRelationship
import streams.utils.JSONUtils
import streams.utils.StreamsUtils
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.fail


class Neo4jSinkTaskTest {

    companion object {
        private lateinit var driver: Driver
        private lateinit var session: Session

        @JvmStatic
        val neo4j = Neo4jContainerExtension()

        @BeforeClass
        @JvmStatic
        fun setUpContainer() {
            neo4j.start()
            driver = neo4j.driver!!
            session = driver.session()
        }

        @AfterClass
        @JvmStatic
        fun tearDownContainer() {
            driver.let { StreamsUtils.closeSafetely(it) }
            session.let { StreamsUtils.closeSafetely(it) }
            StreamsUtils.closeSafetely(neo4j)
        }
    }


    private lateinit var task: SinkTask

    @After
    fun after() {
        session.run("MATCH (n) DETACH DELETE n")
        task.stop()
    }

    @Before
    fun before() {
        task = Neo4jSinkTask()
        task.initialize(mock(SinkTaskContext::class.java))
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

    @Test
    fun `test array of struct`() {
        val firstTopic = "neotopic"
        val props = mutableMapOf<String, String>()
        props[Neo4jConnectorConfig.SERVER_URI] = neo4j.boltUrl
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
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()
        props[Neo4jConnectorConfig.BATCH_SIZE] = 2.toString()
        props[SinkTask.TOPICS_CONFIG] = firstTopic

        task.start(props)
        val input = listOf(SinkRecord(firstTopic, 1, null, null, PERSON_SCHEMA, Neo4jValueConverterTest.getTreeStruct(), 42))
        task.put(input)
        session.beginTransaction().use {
            assertEquals(1, it.findNodes(Label.label("BODY")).stream().count())
            assertEquals(2, it.findNodes(Label.label("P")).stream().count())
            assertEquals(2, it.findNodes(Label.label("UL")).stream().count())
            assertEquals(4, it.findNodes(Label.label("LI")).stream().count())

            assertEquals(2, it.run("MATCH (b:BODY)-[r:HAS_P]->(p:P) RETURN COUNT(r) AS COUNT").single()["COUNT"].asLong())
            assertEquals(2, it.run("MATCH (b:BODY)-[r:HAS_UL]->(ul:UL) RETURN COUNT(r) AS COUNT").single()["COUNT"].asLong())
            assertEquals(4, it.run("MATCH (ul:UL)-[r:HAS_LI]->(li:LI) RETURN COUNT(r) AS COUNT").single()["COUNT"].asLong())

            assertEquals(1, it.run("MATCH (li:LI{class:['ClassA', 'ClassB']}) RETURN COUNT(li) AS COUNT").single()["COUNT"].asLong())
        }
    }

    @Test
    fun `should insert data into Neo4j`() {
        val firstTopic = "neotopic"
        val secondTopic = "foo"
        val props = mutableMapOf<String, String>()
        props[Neo4jConnectorConfig.SERVER_URI] = neo4j.boltUrl
        props["${Neo4jSinkConnectorConfig.TOPIC_CYPHER_PREFIX}$firstTopic"] = "CREATE (n:PersonExt {name: event.firstName, surname: event.lastName})"
        props["${Neo4jSinkConnectorConfig.TOPIC_CYPHER_PREFIX}$secondTopic"] = "CREATE (n:Person {name: event.firstName})"
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()
        props[Neo4jConnectorConfig.BATCH_SIZE] = 2.toString()
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

        task.start(props)
        val input = listOf(SinkRecord(firstTopic, 1, null, null, PERSON_SCHEMA, struct, 42),
                SinkRecord(firstTopic, 1, null, null, PERSON_SCHEMA, struct, 42),
                SinkRecord(firstTopic, 1, null, null, PERSON_SCHEMA, struct, 42),
                SinkRecord(firstTopic, 1, null, null, PERSON_SCHEMA, struct, 42),
                SinkRecord(firstTopic, 1, null, null, PERSON_SCHEMA, struct, 42),
                SinkRecord(secondTopic, 1, null, null, PERSON_SCHEMA, struct, 43))
        task.put(input)
        session.beginTransaction().use {
            val personCount = it.run("MATCH (p:Person) RETURN COUNT(p) as COUNT").single()["COUNT"].asLong()
            val expectedPersonCount = input.filter { it.topic() == secondTopic }.size
            assertEquals(expectedPersonCount, personCount.toInt())
            val personExtCount = it.run("MATCH (p:PersonExt) RETURN COUNT(p) as COUNT").single()["COUNT"].asLong()
            val expectedPersonExtCount = input.filter { it.topic() == firstTopic }.size
            assertEquals(expectedPersonExtCount, personExtCount.toInt())
        }
    }

    @Test
    fun `should insert data into Neo4j from CDC events`() {
        val firstTopic = "neotopic"
        val props = mapOf(Neo4jConnectorConfig.SERVER_URI to neo4j.boltUrl,
                Neo4jSinkConnectorConfig.TOPIC_CDC_SOURCE_ID to firstTopic,
                Neo4jConnectorConfig.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
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

        task.start(props)
        val input = listOf(SinkRecord(firstTopic, 1, null, null, null, cdcDataStart, 42),
                SinkRecord(firstTopic, 1, null, null, null, cdcDataEnd, 43),
                SinkRecord(firstTopic, 1, null, null, null, cdcDataRelationship, 44))
        task.put(input)
        session.beginTransaction().use {
            val result = it.run("""
                MATCH p = (s:User{name:'Andrea', `comp@ny`:'LARUS-BA', sourceId:'0'})
                    -[r:`KNOWS WHO`{since:2014, sourceId:'2'}]->
                    (e:`User Ext`{name:'Michael', `comp@ny`:'Neo4j', sourceId:'1'})
                RETURN count(p) AS count
            """.trimIndent())
            assertTrue { result.hasNext() }
            val count = result.next()[0].asInt()
            assertEquals(1, count)
            assertFalse { result.hasNext() }
        }
    }

    @Test
    fun `should update data into Neo4j from CDC events`() {
        session.beginTransaction().use {
            it.run("""
                CREATE (s:User:OldLabel:SourceEvent{name:'Andrea', `comp@ny`:'LARUS-BA', sourceId:'0'})
                    -[r:`KNOWS WHO`{since:2014, sourceId:'2'}]->
                    (e:`User Ext`:SourceEvent{name:'Michael', `comp@ny`:'Neo4j', sourceId:'1'})
            """.trimIndent())
            it.commit()
        }
        val firstTopic = "neotopic"
        val props = mapOf(Neo4jConnectorConfig.SERVER_URI to neo4j.boltUrl,
                Neo4jSinkConnectorConfig.TOPIC_CDC_SOURCE_ID to firstTopic,
                Neo4jConnectorConfig.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
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

        task.start(props)
        val input = listOf(SinkRecord(firstTopic, 1, null, null, null, cdcDataStart, 42),
                SinkRecord(firstTopic, 1, null, null, null, cdcDataRelationship, 43))
        task.put(input)
        session.beginTransaction().use {
            val result = it.run("""
                MATCH p = (s:User{name:'Andrea', `comp@ny`:'LARUS-BA, Venice', sourceId:'0', age:34})
                    -[r:`KNOWS WHO`{since:2014, sourceId:'2', foo:'bar'}]->
                    (e:`User Ext`{name:'Michael', `comp@ny`:'Neo4j', sourceId:'1'})
                RETURN count(p) AS count
            """.trimIndent())
            assertTrue { result.hasNext() }
            val count = result.next()[0].asInt()
            assertEquals(1, count)
            assertFalse { result.hasNext() }
        }
    }

    @Test
    fun `should insert data into Neo4j from CDC events with schema strategy`() {
        val firstTopic = "neotopic"
        val props = mapOf(Neo4jConnectorConfig.SERVER_URI to neo4j.boltUrl,
                Neo4jSinkConnectorConfig.TOPIC_CDC_SCHEMA to firstTopic,
                Neo4jConnectorConfig.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
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

        task.start(props)
        val input = listOf(SinkRecord(firstTopic, 1, null, null, null, cdcDataStart, 42),
                SinkRecord(firstTopic, 1, null, null, null, cdcDataEnd, 43),
                SinkRecord(firstTopic, 1, null, null, null, cdcDataRelationship, 44))
        task.put(input)
        session.beginTransaction().use {
            val query = """
                |MATCH p = (s:User{name:'Andrea', surname:'Santurbano', `comp@ny`:'LARUS-BA'})-[r:`KNOWS WHO`{since:2014}]->(e:User{name:'Michael', surname:'Hunger', `comp@ny`:'Neo4j'})
                |RETURN count(p) AS count
                |""".trimMargin()
            val result = it.run(query)
            assertTrue { result.hasNext() }
            val count = result.next()[0].asInt()
            assertEquals(1, count)
            assertFalse { result.hasNext() }
        }

        val labels = session.beginTransaction()
            .use { it.allLabels().map(Label::name).toSet() }
        assertEquals(setOf("User"), labels)
    }

    @Test
    fun `should insert data into Neo4j from CDC events with schema strategy, multiple constraints and labels`() {
        val myTopic = UUID.randomUUID().toString()
        val props = mapOf(Neo4jConnectorConfig.SERVER_URI to neo4j.boltUrl,
                Neo4jSinkConnectorConfig.TOPIC_CDC_SCHEMA to myTopic,
                Neo4jConnectorConfig.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
                SinkTask.TOPICS_CONFIG to myTopic)

        val constraintsCharacter = listOf(
                Constraint(label = "Character", type = StreamsConstraintType.UNIQUE, properties = setOf("surname")),
                Constraint(label = "Character", type = StreamsConstraintType.UNIQUE, properties = setOf("name")),
                Constraint(label = "Character", type = StreamsConstraintType.UNIQUE, properties = setOf("country", "address")),
        )
        val constraintsWriter = listOf(
                Constraint(label = "Writer", type = StreamsConstraintType.UNIQUE, properties = setOf("lastName")),
                Constraint(label = "Writer", type = StreamsConstraintType.UNIQUE, properties = setOf("firstName")),
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

        task.start(props)
        val input = listOf(SinkRecord(myTopic, 1, null, null, null, cdcDataStart, 42),
                SinkRecord(myTopic, 1, null, null, null, cdcDataEnd, 43),
                SinkRecord(myTopic, 1, null, null, null, cdcDataRelationship, 44))

        task.put(input)

        session.beginTransaction().use {
            val query = """
                |MATCH p = (s:Character)-[r:`KNOWS WHO`{since: 2014}]->(e:Writer)
                |RETURN count(p) AS count
                |""".trimMargin()
            val result = it.run(query)
            assertTrue { result.hasNext() }
            val count = result.next()[0].asInt()
            assertEquals(1, count)
            assertFalse { result.hasNext() }
        }

        var allNodes = session.beginTransaction().use { it.allNodes().stream().count() }
        assertEquals(2L, allNodes)

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

        session.beginTransaction().use {
            val query = """
                |MATCH p = (s:Character)-[r:`HAS WRITTEN`{since: 1998}]->(e:Writer)
                |RETURN count(p) AS count
                |""".trimMargin()
            val result = it.run(query)
            assertTrue { result.hasNext() }
            val count = result.next()[0].asInt()
            assertEquals(1, count)
            assertFalse { result.hasNext() }
        }
        allNodes = session.beginTransaction().use { it.allNodes().stream().count() }
        assertEquals(4L, allNodes)

    }

    @Test
    fun `should insert data into Neo4j from CDC events with schema strategy and multiple unique constraints merging previous nodes`() {
        val myTopic = UUID.randomUUID().toString()
        val props = mapOf(Neo4jConnectorConfig.SERVER_URI to neo4j.boltUrl,
                Neo4jSinkConnectorConfig.TOPIC_CDC_SCHEMA to myTopic,
                Neo4jConnectorConfig.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
                SinkTask.TOPICS_CONFIG to myTopic)

        val constraints = listOf(
                Constraint(label = "User", type = StreamsConstraintType.UNIQUE, properties = setOf("name")),
                Constraint(label = "User", type = StreamsConstraintType.UNIQUE, properties = setOf("country", "address")),
                Constraint(label = "User", type = StreamsConstraintType.UNIQUE, properties = setOf("surname")),
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

        task.start(props)
        val input = listOf(SinkRecord(myTopic, 1, null, null, null, cdcDataStart, 42),
                SinkRecord(myTopic, 1, null, null, null, cdcDataEnd, 43),
                SinkRecord(myTopic, 1, null, null, null, cdcDataRelationship, 44))

        task.put(input)

        session.beginTransaction().use {
            val query = """
                |MATCH p = (s:User)-[r:`KNOWS WHO` {since: 2014}]->(e:User)
                |RETURN count(p) as count
                |""".trimMargin()
            val result = it.run(query)
            assertTrue { result.hasNext() }
            val count = result.next()[0].asInt()
            assertEquals(1, count)
            assertFalse { result.hasNext() }
        }

        var labels = session.beginTransaction()
            .use { it.allLabels().map(Label::name).toSet() }
        assertEquals(setOf("User"), labels)

        var countUsers = session.beginTransaction().use { it.findNodes(Label.label("User")).stream().count() }
        assertEquals(2L, countUsers)


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

        session.beginTransaction().use {
            val query = """
                |MATCH p = (s:User)-[r:`HAS WRITTEN`{since: 2000}]->(e:User)
                |RETURN count(p) AS count
                |""".trimMargin()
            val result = it.run(query)
            assertTrue { result.hasNext() }
            val count = result.next()[0].asInt()
            assertEquals(1, count)
            assertFalse { result.hasNext() }
        }
        labels = session.beginTransaction()
            .use { it.allLabels().map(Label::name).toSet() }
        assertEquals(setOf("User"), labels)

        countUsers = session.beginTransaction().use { it.allNodes().stream().count() }
        assertEquals(4L, countUsers)
    }

    @Test
    fun `should insert data into Neo4j from CDC events with schema strategy and multiple unique constraints`() {
        val myTopic = UUID.randomUUID().toString()
        val props = mapOf(Neo4jConnectorConfig.SERVER_URI to neo4j.boltUrl,
                Neo4jSinkConnectorConfig.TOPIC_CDC_SCHEMA to myTopic,
                Neo4jConnectorConfig.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
                SinkTask.TOPICS_CONFIG to myTopic)

        val constraints = listOf(
                Constraint(label = "User", type = StreamsConstraintType.UNIQUE, properties = setOf("name")),
                Constraint(label = "User", type = StreamsConstraintType.UNIQUE, properties = setOf("surname")),
                Constraint(label = "User", type = StreamsConstraintType.UNIQUE, properties = setOf("country", "address")),
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
                SinkRecord(myTopic, 1, null, null, null, cdcDataRelationship, 44),
        )
        task.put(input)

        session.beginTransaction().use {
            val query = """
                |MATCH p = (s:User)-[r:`KNOWS WHO` {since: 2014}]->(e:User)
                |RETURN count(p) as count
                |""".trimMargin()
            val result = it.run(query)
            assertTrue { result.hasNext() }
            val count = result.next()[0].asInt()
            assertEquals(1, count)
            assertFalse { result.hasNext() }
        }

        val labels = session.beginTransaction()
            .use { it.allLabels().map(Label::name).toSet() }
        assertEquals(setOf("User"), labels)

        val countUsers = session.beginTransaction().use { it.findNodes(Label.label("User")).stream().count() }
        assertEquals(4L, countUsers)
    }

    @Test
    fun `should delete data into Neo4j from CDC events`() {
        session.beginTransaction().use {
            it.run("""
                CREATE (s:User:OldLabel:SourceEvent{name:'Andrea', `comp@ny`:'LARUS-BA', sourceId:'0'})
                    -[r:`KNOWS WHO`{since:2014, sourceId:'2'}]->
                    (e:`User Ext`:SourceEvent{name:'Michael', `comp@ny`:'Neo4j', sourceId:'1'})
            """.trimIndent())
            it.commit()
        }
        val firstTopic = "neotopic"
        val props = mapOf(Neo4jConnectorConfig.SERVER_URI to neo4j.boltUrl,
                Neo4jSinkConnectorConfig.TOPIC_CDC_SOURCE_ID to firstTopic,
                Neo4jConnectorConfig.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
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
        task.start(props)
        val input = listOf(SinkRecord(firstTopic, 1, null, null, null, cdcDataStart, 42))
        task.put(input)
        session.beginTransaction().use {
            val result = it.run("""
                    MATCH (s:SourceEvent)
                    RETURN count(s) as count
                """.trimIndent())
            assertTrue { result.hasNext() }
            val count = result.next()[0].asInt()
            assertEquals(1, count)
            assertFalse { result.hasNext() }
        }
    }

    @Test
    fun `should not insert data into Neo4j`() {
        val topic = "neotopic"
        val props = mutableMapOf<String, String>()
        props[Neo4jConnectorConfig.SERVER_URI] = neo4j.boltUrl
        props["${Neo4jSinkConnectorConfig.TOPIC_CYPHER_PREFIX}$topic"] = "CREATE (n:Person {name: event.firstName, surname: event.lastName})"
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()
        props[SinkTask.TOPICS_CONFIG] = topic

        val struct= Struct(PLACE_SCHEMA)
                .put("name", "San Mateo (CA)")
                .put("latitude", 37.5629917.toFloat())
                .put("longitude", -122.3255254.toFloat())

        task.start(props)
        task.put(listOf(SinkRecord(topic, 1, null, null, PERSON_SCHEMA, struct, 42)))
        session.beginTransaction().use {
            val node: Node? = it.findNode(Label.label("Person"), "name", "Alex")
            assertTrue { node == null }
        }
    }

    @Test
    fun `should report but not fail parsing data`() {
        val topic = "neotopic"
        val props = mutableMapOf<String, String>()
        props[Neo4jConnectorConfig.SERVER_URI] = neo4j.boltUrl
        props["${Neo4jSinkConnectorConfig.TOPIC_CYPHER_PREFIX}$topic"] = "CREATE (n:Person {name: event.firstName, surname: event.lastName})"
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()
        props[SinkTask.TOPICS_CONFIG] = topic
        props[ErrorService.ErrorConfig.TOLERANCE] = "all"
        props[ErrorService.ErrorConfig.LOG] = true.toString()

        task.start(props)
        task.put(listOf(SinkRecord(topic, 1, null, null, null, "a", 42)))
        session.beginTransaction().use {
            val node: Node? = it.findNode(Label.label("Person"), "name", "Alex")
            assertTrue { node == null }
        }
    }

    @Test
    fun `should report but not fail invalid schema`() {
        val topic = "neotopic"
        val props = mutableMapOf<String, String>()
        props[Neo4jConnectorConfig.SERVER_URI] = neo4j.boltUrl
        props["${Neo4jSinkConnectorConfig.TOPIC_CYPHER_PREFIX}$topic"] = "CREATE (n:Person {name: event.firstName, surname: event.lastName})"
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()
        props[ErrorService.ErrorConfig.TOLERANCE] = "all"
        props[ErrorService.ErrorConfig.LOG] = true.toString()
        props[SinkTask.TOPICS_CONFIG] = topic

        task.start(props)
        task.put(listOf(SinkRecord(topic, 1, null, 42, null, "true", 42)))
        session.beginTransaction().use {
            val node: Node? = it.findNode(Label.label("Person"), "name", "Alex")
            assertTrue { node == null }
        }
    }

    @Test
    fun `should fail running invalid cypher`() {
        val topic = "neotopic"
        val props = mutableMapOf<String, String>()
        props[Neo4jConnectorConfig.SERVER_URI] = neo4j.boltUrl
        props["${Neo4jSinkConnectorConfig.TOPIC_CYPHER_PREFIX}$topic"] = " No Valid Cypher "
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()
        props[SinkTask.TOPICS_CONFIG] = topic
        props[ErrorService.ErrorConfig.TOLERANCE] = "all"
        props[ErrorService.ErrorConfig.LOG] = true.toString()

        task.start(props)
        task.put(listOf(SinkRecord(topic, 1, null, 42, null, "{\"foo\":42}", 42)))
        session.beginTransaction().use {
            val node: Node? = it.findNode(Label.label("Person"), "name", "Alex")
            assertTrue { node == null }
        }
    }

    @Test
    fun `should work with node pattern topic`() {
        val topic = "neotopic"
        val props = mutableMapOf<String, String>()
        props[Neo4jConnectorConfig.SERVER_URI] = neo4j.boltUrl
        props["${Neo4jSinkConnectorConfig.TOPIC_PATTERN_NODE_PREFIX}$topic"] = "User{!userId,name,surname,address.city}"
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()
        props[SinkTask.TOPICS_CONFIG] = topic

        val data = mapOf("userId" to 1, "name" to "Andrea", "surname" to "Santurbano",
                "address" to mapOf("city" to "Venice", "CAP" to "30100"))

        task.start(props)
        val input = listOf(SinkRecord(topic, 1, null, null, null, data, 42))
        task.put(input)
        session.beginTransaction().use {
            val result = it.run("MATCH (n:User{name: 'Andrea', surname: 'Santurbano', userId: 1, `address.city`: 'Venice'}) RETURN count(n) AS count")
            assertTrue { result.hasNext() }
            val count = result.next()[0].asInt()
            assertEquals(1, count)
            assertFalse { result.hasNext() }
        }
    }

    @Test
    fun `should work with relationship pattern topic`() {
        val topic = "neotopic"
        val props = mutableMapOf<String, String>()
        props[Neo4jConnectorConfig.SERVER_URI] = neo4j.boltUrl
        props["${Neo4jSinkConnectorConfig.TOPIC_PATTERN_RELATIONSHIP_PREFIX}$topic"] = "(:User{!sourceId,sourceName,sourceSurname})-[:KNOWS]->(:User{!targetId,targetName,targetSurname})"
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()
        props[SinkTask.TOPICS_CONFIG] = topic

        val data = mapOf("sourceId" to 1, "sourceName" to "Andrea", "sourceSurname" to "Santurbano",
                "targetId" to 1, "targetName" to "Michael", "targetSurname" to "Hunger", "since" to 2014)

        task.start(props)
        val input = listOf(SinkRecord(topic, 1, null, null, null, data, 42))
        task.put(input)
        session.beginTransaction().use {
            val result = it.run("""
                MATCH p = (s:User{sourceName: 'Andrea', sourceSurname: 'Santurbano', sourceId: 1})-[:KNOWS{since: 2014}]->(e:User{targetName: 'Michael', targetSurname: 'Hunger', targetId: 1})
                RETURN count(p) AS count
            """.trimIndent())
            assertTrue { result.hasNext() }
            val count = result.next()[0].asInt()
            assertEquals(1, count)
            assertFalse { result.hasNext() }
        }
    }

    @Test
    fun `should work with node pattern topic for tombstone record`() {
        session.beginTransaction().use {
            it.run("CREATE (u:User{userId: 1, name: 'Andrea', surname: 'Santurbano'})")
            it.commit()
        }
        val count = session.beginTransaction().use {
            it.run("MATCH (n) RETURN count(n) AS count")
                    .single()["count"].asLong()
        }
        assertEquals(1L, count)
        val topic = "neotopic"
        val props = mutableMapOf<String, String>()
        props[Neo4jConnectorConfig.SERVER_URI] = neo4j.boltUrl
        props["${Neo4jSinkConnectorConfig.TOPIC_PATTERN_NODE_PREFIX}$topic"] = "User{!userId,name,surname,address.city}"
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()
        props[SinkTask.TOPICS_CONFIG] = topic

        val data = mapOf("userId" to 1)

        task.start(props)
        val input = listOf(SinkRecord(topic, 1, null, data, null, null, 42))
        task.put(input)
        session.beginTransaction().use {
            val result = it.run("MATCH (n) RETURN count(n) AS count")
            assertTrue { result.hasNext() }
            val count = result.next()[0].asInt()
            assertEquals(0, count)
            assertFalse { result.hasNext() }
        }
    }

    @Test
    fun `should create and delete relationship from CUD event without properties field`() {
        val relType = "MY_REL"
        val key = "key"
        val startNode = "SourceNode"
        val endNode = "TargetNode"
        val topic = UUID.randomUUID().toString()

        session.beginTransaction().use {
            it.run("CREATE (:$startNode {key: 1}) CREATE (:$endNode {key: 1})")
            it.commit()
        }
        
        val start = CUDNodeRel(ids = mapOf(key to 1), labels = listOf(startNode))
        val end = CUDNodeRel(ids = mapOf(key to 1), labels = listOf(endNode))
        val relMerge = CUDRelationship(op = CUDOperations.merge, from = start, to = end, rel_type = relType)
        val sinkRecordMerge = SinkRecord(topic, 1, null, null, null, JSONUtils.asMap(relMerge), 0L)

        val props = mutableMapOf<String, String>()
        props[Neo4jConnectorConfig.SERVER_URI] = neo4j.boltUrl
        props[Neo4jSinkConnectorConfig.TOPIC_CUD] = topic
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()
        props[SinkTask.TOPICS_CONFIG] = topic

        task.start(props)
        task.put(listOf(sinkRecordMerge))

        val queryCount = "MATCH p = (:$startNode)-[:$relType]->(:$endNode) RETURN count(p) AS count"
        
        session.beginTransaction().use {
            val countRels = it.run(queryCount)
                .single()[0].asLong()
            assertEquals(1L, countRels)
        }

        val relDelete = CUDRelationship(op = CUDOperations.delete, from = start, to = end, rel_type = relType)
        val sinkRecordDelete = SinkRecord(topic, 1, null, null, null, JSONUtils.asMap(relDelete), 1L)
        task.put(listOf(sinkRecordDelete))

        session.beginTransaction().use {
            val countRels = it.run(queryCount)
                .single()[0].asLong()
            assertEquals(0L, countRels)
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
        props[Neo4jConnectorConfig.SERVER_URI] = neo4j.boltUrl
        props[Neo4jSinkConnectorConfig.TOPIC_CUD] = topic
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()
        props[SinkTask.TOPICS_CONFIG] = topic

        // when
        task.start(props)
        task.put(data)

        // then
        session.beginTransaction().use {
            val countFooBarLabel = it.run("MATCH (n:Foo:Bar:Label) RETURN count(n) AS count")
                    .single()[0].asLong()
            assertEquals(5L, countFooBarLabel)
            val countFooBar = it.run("MATCH (n:Foo:Bar) RETURN count(n) AS count")
                    .single()[0].asLong()
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
        props[Neo4jConnectorConfig.SERVER_URI] = neo4j.boltUrl
        props[Neo4jSinkConnectorConfig.TOPIC_CUD] = topic
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()
        props[SinkTask.TOPICS_CONFIG] = topic
        session.beginTransaction().use {
            it.run("""
                UNWIND range(1, 10) AS id
                CREATE (:Foo:Bar {key: id})
                CREATE (:FooBar {key: id})
            """.trimIndent())
            assertEquals(0, it.allRelationships().stream().count().toInt())
            assertEquals(20, it.allNodes().stream().count().toInt())
            it.commit()
        }

        // when
        task.start(props)
        task.put(data)

        // then
        session.beginTransaction().use {
            val countFooBarLabel = it.run("""
                MATCH (:Foo:Bar)-[r:$rel_type]->(:FooBar)
                RETURN count(r) AS count
            """.trimIndent())
                    .single()["count"].asLong()
            assertEquals(10L, countFooBarLabel)
        }
    }

    @Test
    fun `should create nodes and relationship, if one or both nodes doesn't exist from CUD Events`() {
        // given
        val key = "key"
        val topic = UUID.randomUUID().toString()
        val relType = "MY_REL"
        val data = (1..10).map {
            val properties = mapOf("foo" to "foo-value-$it", "id" to it)
            val start = CUDNodeRel(ids = mapOf(key to it), labels = listOf("Foo", "Bar"), op = CUDOperations.merge)
            val end = CUDNodeRel(ids = mapOf(key to it), labels = listOf("FooBar"), op = CUDOperations.merge)
            val rel = CUDRelationship(op = CUDOperations.merge, properties = properties, from = start, to = end, rel_type = relType)
            SinkRecord(topic, 1, null, null, null, JSONUtils.asMap(rel), it.toLong())
        }
        val props = mutableMapOf<String, String>()
        props[Neo4jConnectorConfig.SERVER_URI] = neo4j.boltUrl
        props[Neo4jSinkConnectorConfig.TOPIC_CUD] = topic
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()
        props[SinkTask.TOPICS_CONFIG] = topic

        // when
        task.start(props)
        task.put(data)

        // then
        session.beginTransaction().use {
            val countFooBarLabel = it.run("""
                MATCH (:Foo:Bar)-[r:$relType]->(:FooBar)
                RETURN count(r) AS count
            """.trimIndent())
                    .single()["count"].asLong()
            assertEquals(10L, countFooBarLabel)
        }

        // now, I create only start nodes
        val dataWithStartPreset = (11..20).map {
            val properties = mapOf("foo" to "foo-value-$it", "id" to it)
            val start = CUDNodeRel(ids = mapOf(key to it), labels = listOf("Foo", "Bar"))
            val end = CUDNodeRel(ids = mapOf(key to it), labels = listOf("FooBar"), op = CUDOperations.merge)
            val rel = CUDRelationship(op = CUDOperations.merge, properties = properties, from = start, to = end, rel_type = relType)
            SinkRecord(topic, 1, null, null, null, JSONUtils.asMap(rel), it.toLong())
        }

        session.beginTransaction().use {
            it.run("""
                UNWIND range(11, 20) AS id
                CREATE (:Foo:Bar {key: id})
            """.trimIndent())
            assertEquals(10, it.allRelationships().stream().count().toInt())
            assertEquals(30, it.allNodes().stream().count().toInt())
            it.commit()
        }

        task.put(dataWithStartPreset)

        session.beginTransaction().use {
            val countFooBarLabel = it.run("""
                MATCH (:Foo:Bar)-[r:$relType]->(:FooBar)
                RETURN count(r) AS count
            """.trimIndent())
                    .single()["count"].asLong()
            assertEquals(20L, countFooBarLabel)
        }

        // now, I create only end nodes
        val dataWithEndPreset = (21..30).map {
            val properties = mapOf("foo" to "foo-value-$it", "id" to it)
            val start = CUDNodeRel(ids = mapOf(key to it), labels = listOf("Foo", "Bar"), op = CUDOperations.merge)
            val end = CUDNodeRel(ids = mapOf(key to it), labels = listOf("FooBar"))
            val rel = CUDRelationship(op = CUDOperations.merge, properties = properties, from = start, to = end, rel_type = relType)
            SinkRecord(topic, 1, null, null, null, JSONUtils.asMap(rel), it.toLong())
        }

        session.beginTransaction().use {
            it.run("""
                UNWIND range(21, 30) AS id
                CREATE (:FooBar {key: id})
            """.trimIndent())
            assertEquals(20, it.allRelationships().stream().count().toInt())
            assertEquals(50, it.allNodes().stream().count().toInt())
            it.commit()
        }

        task.put(dataWithEndPreset)

        session.beginTransaction().use {
            val countFooBarLabel = it.run("""
                MATCH (:Foo:Bar)-[r:$relType]->(:FooBar)
                RETURN count(r) AS count
            """.trimIndent())
                    .single()["count"].asLong()
            assertEquals(30L, countFooBarLabel)
        }

    }

    @Test
    fun `should create entities only with valid CUD operations`() {
        // given
        val invalidMarkers = listOf(3, 6, 9)
        val key = "key"
        val topic = UUID.randomUUID().toString()
        val data = (1..10).map {
            val labels = listOf("Foo", "Bar", "Label")
            val properties = mapOf("foo" to "foo-value-$it", "id" to it)
            val (op, ids) = when (it) {
                in invalidMarkers -> CUDOperations.match to mapOf(key to it)
                else -> CUDOperations.create to emptyMap()
            }
            val cudNode = CUDNode(op = op,
                    labels = labels,
                    ids = ids,
                    properties = properties)
            SinkRecord(topic, 1, null, null, null, JSONUtils.asMap(cudNode), it.toLong())
        }

        val relType = "MY_REL"
        val invalidRelMarkers = listOf(1, 4)
        val invalidNodeRelMarkers = listOf(3, 6, 7)
        val dataRel = (1..10).map {
            val properties = mapOf("foo" to "foo-value-$it", "id" to it)
            val opRelationship = if (it in invalidRelMarkers) CUDOperations.delete else CUDOperations.merge
            val opStartNode = if (it in invalidNodeRelMarkers) CUDOperations.delete else CUDOperations.merge
            val start = CUDNodeRel(ids = mapOf(key to it), labels = listOf("Foo", "Bar"), op = opStartNode)
            val end = CUDNodeRel(ids = mapOf(key to it), labels = listOf("FooBar"), op = CUDOperations.merge)
            val rel = CUDRelationship(op = opRelationship, properties = properties, from = start, to = end, rel_type = relType)
            SinkRecord(topic, 1, null, null, null, JSONUtils.asMap(rel), it.toLong())
        }

        val props = mutableMapOf<String, String>()
        props[Neo4jConnectorConfig.SERVER_URI] = neo4j.boltUrl
        props[Neo4jSinkConnectorConfig.TOPIC_CUD] = topic
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()
        props[SinkTask.TOPICS_CONFIG] = topic

        // when
        task.start(props)
        task.put(data)
        task.put(dataRel)

        // then
        session.beginTransaction().use {
            val countFooBarLabel = it.run("MATCH (n:Foo:Bar:Label) RETURN count(n) AS count")
                .single()["count"].asLong()
            assertEquals(7L, countFooBarLabel)
            val countRelationships = it.run("""
                MATCH (:Foo:Bar)-[r:$relType]->(:FooBar)
                RETURN count(r) AS count
            """.trimIndent())
                    .single()["count"].asLong()
            assertEquals(5L, countRelationships)
        }
    }

    @Test
    fun `should fail data insertion with ProcessingError`() {
        // given
        val topic = UUID.randomUUID().toString()

        val props = mutableMapOf<String, String>()
        props[Neo4jConnectorConfig.SERVER_URI] = neo4j.boltUrl
        props["${Neo4jSinkConnectorConfig.TOPIC_PATTERN_RELATIONSHIP_PREFIX}$topic"] = "(:User{!sourceId,sourceName,sourceSurname})-[:KNOWS]->(:User{!targetId,targetName,targetSurname})"
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()
        props[SinkTask.TOPICS_CONFIG] = topic
        props[Neo4jConnectorConfig.DATABASE] = "notExistent"

        val data = mapOf("sourceId" to 1, "sourceName" to "Andrea", "sourceSurname" to "Santurbano",
                "targetId" to 1, "targetName" to "Michael", "targetSurname" to "Hunger", "since" to 2014)

        task.start(props)
        val input = listOf(SinkRecord(topic, 1, null, null, null, data, 42))

        try {
            task.put(input)
            fail("It should fail with ProcessingError")
        } catch (e: ProcessingError) {
            val errorData = e.errorDatas.first()
            assertTrue(errorData.databaseName == "notExistent"
                    && errorData.exception!!.javaClass.name == "org.neo4j.driver.exceptions.FatalDiscoveryException")
        }

        props[Neo4jConnectorConfig.DATABASE] = "neo4j"
        val taskNotValid = Neo4jSinkTask()
        taskNotValid.initialize(mock(SinkTaskContext::class.java))
        taskNotValid.start(props)

        val dataNotValid = mapOf("sourceId" to null, "sourceName" to "Andrea", "sourceSurname" to "Santurbano",
                "targetId" to 1, "targetName" to "Michael", "targetSurname" to "Hunger", "since" to 2014)
        val inputNotValid = listOf(SinkRecord(topic, 1, null, null, null, dataNotValid, 43))

        try {
            taskNotValid.put(inputNotValid)
            fail("It should fail with ProcessingError")
        } catch (e: ProcessingError) {
            val errorData = e.errorDatas.first()
            assertTrue(errorData.databaseName == "neo4j"
                    && errorData.exception!!.javaClass.name == "org.neo4j.driver.exceptions.ClientException")
        }
    }
    
    @Test
    @Ignore("flaky")
    fun `should stop the query and fails with small timeout and vice versa`() {
        val myTopic = "foo"
        val props = mutableMapOf<String, String>()
        props[Neo4jConnectorConfig.SERVER_URI] = neo4j.boltUrl
        props["${Neo4jSinkConnectorConfig.TOPIC_CYPHER_PREFIX}$myTopic"] = "CREATE (n:Person {name: event.name})"
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()
        props[Neo4jSinkConnectorConfig.BATCH_PARALLELIZE] = true.toString()
        val batchSize = 500000
        props[Neo4jConnectorConfig.BATCH_SIZE] = batchSize.toString()
        props[Neo4jConnectorConfig.BATCH_TIMEOUT_MSECS] = 1.toString()
        props[SinkTask.TOPICS_CONFIG] = myTopic
        val input = (1..batchSize).map {
            SinkRecord(myTopic, 1, null, null, null, mapOf("name" to it.toString()), it.toLong())
        }
        // test timeout with parallel=true
        assertFailsWithTimeout(props, input, batchSize)
        countFooPersonEntities(0)

        // test timeout with parallel=false
        props[Neo4jSinkConnectorConfig.BATCH_PARALLELIZE] = false.toString()
        assertFailsWithTimeout(props, input, batchSize)
        countFooPersonEntities(0)

        // test with large timeout
        props[Neo4jConnectorConfig.BATCH_TIMEOUT_MSECS] = 30000.toString()
        val taskValidParallelFalse = Neo4jSinkTask()
        taskValidParallelFalse.initialize(mock(SinkTaskContext::class.java))
        taskValidParallelFalse.start(props)
        taskValidParallelFalse.put(input)         
        countFooPersonEntities(batchSize)

        props[Neo4jSinkConnectorConfig.BATCH_PARALLELIZE] = true.toString()
        val taskValidParallelTrue = Neo4jSinkTask()
        taskValidParallelTrue.initialize(mock(SinkTaskContext::class.java))
        taskValidParallelTrue.start(props)
        taskValidParallelTrue.put(input)         
        countFooPersonEntities(batchSize * 2)
    }

    private fun assertFailsWithTimeout(props: MutableMap<String, String>, input: List<SinkRecord>, expectedDataErrorSize: Int) {
        try {
            val taskInvalid = Neo4jSinkTask()
            taskInvalid.initialize(mock(SinkTaskContext::class.java))
            taskInvalid.start(props)
            taskInvalid.put(input)
            fail("Should fail because of TimeoutException")
        } catch (e: ProcessingError) {
            val errors = e.errorDatas
            assertEquals(expectedDataErrorSize, errors.size)
        }
    }

    private fun countFooPersonEntities(expected: Int) {
        val personCount = session.run("MATCH (p:Person) RETURN count(p) as count")
            .single()["count"]
            .asLong()
        assertEquals(expected, personCount.toInt())
    }


}