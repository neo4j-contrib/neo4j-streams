package streams.kafka.connect.sink

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.Timestamp
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask
import org.apache.kafka.connect.sink.SinkTaskContext
import org.easymock.EasyMockSupport
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.harness.ServerControls
import org.neo4j.harness.TestServerBuilders
import streams.events.*
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue


class Neo4jSinkTaskTest: EasyMockSupport() {

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
        task.initialize(mock<SinkTaskContext, SinkTaskContext>(SinkTaskContext::class.java))
        task.start(props)
        val input = listOf(SinkRecord(firstTopic, 1, null, null, PERSON_SCHEMA, struct, 42),
                SinkRecord(firstTopic, 1, null, null, PERSON_SCHEMA, struct, 42),
                SinkRecord(firstTopic, 1, null, null, PERSON_SCHEMA, struct, 43),
                SinkRecord(firstTopic, 1, null, null, PERSON_SCHEMA, struct, 44),
                SinkRecord(firstTopic, 1, null, null, PERSON_SCHEMA, struct, 45),
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
        val props = mutableMapOf<String, String>()
        props[Neo4jSinkConnectorConfig.SERVER_URI] = db.boltURI().toString()
        props["${Neo4jSinkConnectorConfig.TOPIC_CDC}"] = firstTopic
        props[Neo4jSinkConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()
        props[SinkTask.TOPICS_CONFIG] = firstTopic

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
                start = RelationshipNodeChange(id = "0", labels = listOf("User")),
                end = RelationshipNodeChange(id = "1", labels = listOf("User Ext")),
                after = RelationshipChange(properties = mapOf("since" to 2014)),
                before = null,
                label = "KNOWS WHO"
            ),
            schema = Schema()
        )

        val task = Neo4jSinkTask()
        task.initialize(mock<SinkTaskContext, SinkTaskContext>(SinkTaskContext::class.java))
        task.start(props)
        val input = listOf(SinkRecord(firstTopic, 1, null, null, null, cdcDataStart, 42),
                SinkRecord(firstTopic, 1, null, null, null, cdcDataEnd, 43),
                SinkRecord(firstTopic, 1, null, null, null, cdcDataRelationship, 44))
        task.put(input)
        db.graph().beginTx().use {
            db.graph().execute("""
                MATCH p = (s:User{name:'Andrea', `comp@ny`:'LARUS-BA', streams_id:'0'})
                    -[r:`KNOWS WHO`{since:2014, streams_id:'2'}]->
                    (e:`User Ext`{name:'Michael', `comp@ny`:'Neo4j', streams_id:'1'})
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
                CREATE (s:User:OldLabel:StreamsEvent{name:'Andrea', `comp@ny`:'LARUS-BA', streams_id:'0'})
                    -[r:`KNOWS WHO`{since:2014, streams_id:'2'}]->
                    (e:`User Ext`:StreamsEvent{name:'Michael', `comp@ny`:'Neo4j', streams_id:'1'})
            """.trimIndent()).close()
            it.success()
        }
        val firstTopic = "neotopic"
        val props = mutableMapOf<String, String>()
        props[Neo4jSinkConnectorConfig.SERVER_URI] = db.boltURI().toString()
        props["${Neo4jSinkConnectorConfig.TOPIC_CDC}"] = firstTopic
        props[Neo4jSinkConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()
        props[SinkTask.TOPICS_CONFIG] = firstTopic

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
                start = RelationshipNodeChange(id = "0", labels = listOf("User")),
                end = RelationshipNodeChange(id = "1", labels = listOf("User Ext")),
                after = RelationshipChange(properties = mapOf("since" to 2014, "foo" to "bar")),
                before = RelationshipChange(properties = mapOf("since" to 2014)),
                label = "KNOWS WHO"
            ),
            schema = Schema()
        )

        val task = Neo4jSinkTask()
        task.initialize(mock<SinkTaskContext, SinkTaskContext>(SinkTaskContext::class.java))
        task.start(props)
        val input = listOf(SinkRecord(firstTopic, 1, null, null, null, cdcDataStart, 42),
                SinkRecord(firstTopic, 1, null, null, null, cdcDataRelationship, 43))
        task.put(input)
        db.graph().beginTx().use {
            db.graph().execute("""
                MATCH p = (s:User{name:'Andrea', `comp@ny`:'LARUS-BA, Venice', streams_id:'0', age:34})
                    -[r:`KNOWS WHO`{since:2014, streams_id:'2', foo:'bar'}]->
                    (e:`User Ext`{name:'Michael', `comp@ny`:'Neo4j', streams_id:'1'})
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
    fun `should delete data into Neo4j from CDC events`() {
        db.graph().beginTx().use {
            db.graph().execute("""
                CREATE (s:User:OldLabel:StreamsEvent{name:'Andrea', `comp@ny`:'LARUS-BA', streams_id:'0'})
                    -[r:`KNOWS WHO`{since:2014, streams_id:'2'}]->
                    (e:`User Ext`:StreamsEvent{name:'Michael', `comp@ny`:'Neo4j', streams_id:'1'})
            """.trimIndent()).close()
            it.success()
        }
        val firstTopic = "neotopic"
        val props = mutableMapOf<String, String>()
        props[Neo4jSinkConnectorConfig.SERVER_URI] = db.boltURI().toString()
        props["${Neo4jSinkConnectorConfig.TOPIC_CDC}"] = firstTopic
        props[Neo4jSinkConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()
        props[SinkTask.TOPICS_CONFIG] = firstTopic

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
        task.initialize(mock<SinkTaskContext, SinkTaskContext>(SinkTaskContext::class.java))
        task.start(props)
        val input = listOf(SinkRecord(firstTopic, 1, null, null, null, cdcDataStart, 42))
        task.put(input)
        db.graph().beginTx().use {
            db.graph().execute("""
                    MATCH (s:StreamsEvent)
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
        task.initialize(mock<SinkTaskContext, SinkTaskContext>(SinkTaskContext::class.java))
        task.start(props)
        task.put(listOf(SinkRecord(topic, 1, null, null, PERSON_SCHEMA, struct, 42)))
        db.graph().beginTx().use {
            val node: Node? = db.graph().findNode(Label.label("Person"), "name", "Alex")
            assertTrue { node == null }
        }
    }
}