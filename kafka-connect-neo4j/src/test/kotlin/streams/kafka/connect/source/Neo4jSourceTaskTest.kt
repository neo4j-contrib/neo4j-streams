package streams.kafka.connect.source

import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask
import org.apache.kafka.connect.source.SourceTaskContext
import org.apache.kafka.connect.storage.OffsetStorageReader
import org.hamcrest.Matchers
import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.mockito.Mockito
import org.neo4j.driver.Driver
import org.neo4j.driver.Session
import org.neo4j.function.ThrowingSupplier
import streams.Assert
import streams.Neo4jContainerExtension
import streams.kafka.connect.common.Neo4jConnectorConfig
import streams.kafka.connect.sink.AuthenticationType
import streams.utils.JSONUtils
import streams.utils.StreamsUtils
import java.util.*
import java.util.concurrent.TimeUnit

class Neo4jSourceTaskTest {

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

    private lateinit var task: SourceTask

    @After
    fun after() {
        session.run("MATCH (n) DETACH DELETE n")
        task.stop()
    }

    @Before
    fun before() {
        task = Neo4jSourceTask()
        val sourceTaskContextMock = Mockito.mock(SourceTaskContext::class.java)
        val offsetStorageReader = Mockito.mock(OffsetStorageReader::class.java)
        Mockito.`when`(sourceTaskContextMock.offsetStorageReader())
            .thenReturn(offsetStorageReader)
        Mockito.`when`(offsetStorageReader.offset(Mockito.anyMap<String, Any>()))
            .thenReturn(emptyMap())
        task.initialize(sourceTaskContextMock)
    }

    private fun structToMap(struct: Struct): Map<String, Any?> = struct.schema().fields()
        .map {
            it.name() to when (val value = struct[it.name()]) {
                is Struct -> structToMap(value)
                else -> value
            }
        }
        .toMap()

    fun Struct.toMap() = structToMap(this)

    @Test
    fun `should source data from Neo4j with custom QUERY from NOW`() {
        val props = mutableMapOf<String, String>()
        props[Neo4jConnectorConfig.SERVER_URI] = neo4j.boltUrl
        props[Neo4jSourceConnectorConfig.TOPIC] = UUID.randomUUID().toString()
        props[Neo4jSourceConnectorConfig.STREAMING_POLL_INTERVAL] = "1000"
        props[Neo4jSourceConnectorConfig.STREAMING_PROPERTY] = "timestamp"
        props[Neo4jSourceConnectorConfig.SOURCE_TYPE_QUERY] = getSourceQuery()
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

        task.start(props)
        val totalRecords = 10
        val expected = insertRecords(totalRecords, true)

        val list = mutableListOf<SourceRecord>()
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            task.poll()?.let { list.addAll(it) }
            val actualList = list.map { JSONUtils.readValue<Map<String, Any?>>(it.value()) }
            expected.containsAll(actualList)
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun `should source data from Neo4j with custom QUERY from NOW with Schema`() {
        val props = mutableMapOf<String, String>()
        props[Neo4jConnectorConfig.SERVER_URI] = neo4j.boltUrl
        props[Neo4jSourceConnectorConfig.TOPIC] = UUID.randomUUID().toString()
        props[Neo4jSourceConnectorConfig.STREAMING_POLL_INTERVAL] = "1000"
        props[Neo4jSourceConnectorConfig.ENFORCE_SCHEMA] = "true"
        props[Neo4jSourceConnectorConfig.STREAMING_PROPERTY] = "timestamp"
        props[Neo4jSourceConnectorConfig.SOURCE_TYPE_QUERY] = getSourceQuery()
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

        task.start(props)
        val totalRecords = 10
        val expected = insertRecords(totalRecords)

        val list = mutableListOf<SourceRecord>()
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            task.poll()?.let { list.addAll(it) }
            val actualList = list.map { (it.value() as Struct).toMap() }
            expected.containsAll(actualList)
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun `should source data from Neo4j with custom QUERY from ALL`() {
        val props = mutableMapOf<String, String>()
        props[Neo4jConnectorConfig.SERVER_URI] = neo4j.boltUrl
        props[Neo4jSourceConnectorConfig.TOPIC] = UUID.randomUUID().toString()
        props[Neo4jSourceConnectorConfig.STREAMING_FROM] = "ALL"
        props[Neo4jSourceConnectorConfig.STREAMING_POLL_INTERVAL] = "1000"
        props[Neo4jSourceConnectorConfig.STREAMING_PROPERTY] = "timestamp"
        props[Neo4jSourceConnectorConfig.SOURCE_TYPE_QUERY] = getSourceQuery()
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

        task.start(props)
        val totalRecords = 10
        val expected = insertRecords(totalRecords, true)

        val list = mutableListOf<SourceRecord>()
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            task.poll()?.let { list.addAll(it) }
            val actualList = list.map { JSONUtils.readValue<Map<String, Any?>>(it.value()) }
            expected == actualList
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun `should source data from Neo4j with custom QUERY from ALL with Schema`() {
        val props = mutableMapOf<String, String>()
        props[Neo4jConnectorConfig.SERVER_URI] = neo4j.boltUrl
        props[Neo4jSourceConnectorConfig.TOPIC] = UUID.randomUUID().toString()
        props[Neo4jSourceConnectorConfig.STREAMING_FROM] = "ALL"
        props[Neo4jSourceConnectorConfig.STREAMING_POLL_INTERVAL] = "1000"
        props[Neo4jSourceConnectorConfig.ENFORCE_SCHEMA] = "true"
        props[Neo4jSourceConnectorConfig.STREAMING_PROPERTY] = "timestamp"
        props[Neo4jSourceConnectorConfig.SOURCE_TYPE_QUERY] = getSourceQuery()
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

        task.start(props)
        val totalRecords = 10
        val expected = insertRecords(totalRecords)

        val list = mutableListOf<SourceRecord>()
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            task.poll()?.let { list.addAll(it) }
            val actualList = list.map { (it.value() as Struct).toMap() }
            expected == actualList
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    private fun insertRecords(totalRecords: Int, longToInt: Boolean = false) = session.beginTransaction().use { tx ->
        val elements = (1..totalRecords).map {
            val result = tx.run(
                """
                                |CREATE (n:Test{
                                |   name: 'Name ' + $it,
                                |   timestamp: timestamp(),
                                |   point: point({longitude: 56.7, latitude: 12.78, height: 8}),
                                |   array: [1,2,3],
                                |   datetime: localdatetime(),
                                |   boolean: true
                                |})
                                |RETURN n.name AS name, n.timestamp AS timestamp,
                                |   n.point AS point,
                                |   n.array AS array,
                                |   n.datetime AS datetime,
                                |   n.boolean AS boolean,
                                |   {
                                |       key1: "value1",
                                |       key2: "value2"
                                |   } AS map,
                                |   n AS node
                            """.trimMargin()
            )
            val next = result.next()
            val map = next.asMap().toMutableMap()
            map["array"] = next["array"].asList()
                .map { if (longToInt) (it as Long).toInt() else it }
            map["point"] = JSONUtils.readValue<Map<String, Any>>(map["point"]!!)
            map["datetime"] = next["datetime"].asLocalDateTime().toString()
            val node = next["node"].asNode()
            val nodeMap = node.asMap().toMutableMap()
            nodeMap["<id>"] = if (longToInt) node.id().toInt() else node.id()
            nodeMap["<labels>"] = node.labels()
            // are the same value as above
            nodeMap["array"] = map["array"]
            nodeMap["point"] = map["point"]
            nodeMap["datetime"] = map["datetime"]
            map["node"] = nodeMap
            map
        }
        tx.commit()
        elements
    }

    @Test
    fun `should source data from Neo4j with custom QUERY without streaming property`() {
        val props = mutableMapOf<String, String>()
        props[Neo4jConnectorConfig.SERVER_URI] = neo4j.boltUrl
        props[Neo4jSourceConnectorConfig.TOPIC] = UUID.randomUUID().toString()
        props[Neo4jSourceConnectorConfig.STREAMING_POLL_INTERVAL] = "1000"
        props[Neo4jSourceConnectorConfig.SOURCE_TYPE_QUERY] = getSourceQuery()
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

        task.start(props)
        val totalRecords = 10
        insertRecords(totalRecords)

        val list = mutableListOf<SourceRecord>()
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            task.poll()?.let { list.addAll(it) }
            val actualList = list.map { JSONUtils.readValue<Map<String, Any?>>(it.value()) }
            actualList.size >= 2
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun `should source data from Neo4j with custom QUERY without streaming property with Schema`() {
        val props = mutableMapOf<String, String>()
        props[Neo4jConnectorConfig.SERVER_URI] = neo4j.boltUrl
        props[Neo4jSourceConnectorConfig.TOPIC] = UUID.randomUUID().toString()
        props[Neo4jSourceConnectorConfig.STREAMING_POLL_INTERVAL] = "1000"
        props[Neo4jSourceConnectorConfig.ENFORCE_SCHEMA] = "true"
        props[Neo4jSourceConnectorConfig.SOURCE_TYPE_QUERY] = getSourceQuery()
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

        task.start(props)
        val totalRecords = 10
        insertRecords(totalRecords)

        val list = mutableListOf<SourceRecord>()
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            task.poll()?.let { list.addAll(it) }
            val actualList = list.map { (it.value() as Struct).toMap() }
            actualList.size >= 2
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    private fun getSourceQuery() = """
                |MATCH (n:Test)
                |WHERE n.timestamp > ${'$'}lastCheck
                |RETURN n.name AS name,
                |   n.timestamp AS timestamp,
                |   n.point AS point,
                |   n.array AS array,
                |   n.datetime AS datetime,
                |   n.boolean AS boolean,
                |   {
                |       key1: "value1",
                |       key2: "value2"
                |   } AS map,
                |   n AS node
            """.trimMargin()

    @Test(expected = ConnectException::class)
    fun `should throw exception`() {
        val props = mutableMapOf<String, String>()
        props[Neo4jConnectorConfig.SERVER_URI] = neo4j.boltUrl
        props[Neo4jSourceConnectorConfig.TOPIC] = UUID.randomUUID().toString()
        props[Neo4jSourceConnectorConfig.STREAMING_POLL_INTERVAL] = "1000"
        props[Neo4jSourceConnectorConfig.SOURCE_TYPE_QUERY] = "WRONG QUERY".trimMargin()
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

        task.start(props)
        val totalRecords = 10
        insertRecords(totalRecords)

        var exception: ConnectException? = null
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            try {
                task.poll()
                false
            } catch (e: ConnectException) {
                exception = e
                true
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
        if (exception != null) throw exception as ConnectException
    }

    @Test
    fun `should source data from mock with custom QUERY without streaming property with Schema`() {
        val props = mutableMapOf<String, String>()
        props[Neo4jConnectorConfig.SERVER_URI] = neo4j.boltUrl
        props[Neo4jSourceConnectorConfig.TOPIC] = UUID.randomUUID().toString()
        props[Neo4jSourceConnectorConfig.STREAMING_POLL_INTERVAL] = "1000"
        props[Neo4jSourceConnectorConfig.ENFORCE_SCHEMA] = "true"
        props[Neo4jSourceConnectorConfig.SOURCE_TYPE_QUERY] = """
                |WITH
                |{
                |   id: 'ROOT_ID',
                |   root: [
                |       { children: [] },
                |       { children: [{ name: "child" }] }
                |   ],
                |   arr: [null, {foo: "bar"}]
                |} AS data
                |RETURN data, data.id AS id
            """.trimMargin()
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

        task.start(props)
        val totalRecords = 10
        insertRecords(totalRecords)

        val expected = mapOf(
            "id" to "ROOT_ID",
            "data" to mapOf(
                "id" to "ROOT_ID",
                "arr" to listOf(null, mapOf("foo" to "bar")),
                "root" to listOf(
                    mapOf("children" to emptyList<Map<String, Any>>()),
                    mapOf("children" to listOf(mapOf("name" to "child")))
                )
            )
        )

        Assert.assertEventually(ThrowingSupplier {
            task.poll()?.map { (it.value() as Struct).toMap() }?.first()
        }, Matchers.equalTo(expected), 30, TimeUnit.SECONDS)
    }

    @Test
    fun `should support null values returned from query`() {
        val props = mutableMapOf<String, String>()
        props[Neo4jConnectorConfig.SERVER_URI] = neo4j.boltUrl
        props[Neo4jSourceConnectorConfig.TOPIC] = UUID.randomUUID().toString()
        props[Neo4jSourceConnectorConfig.STREAMING_POLL_INTERVAL] = "1000"
        props[Neo4jSourceConnectorConfig.ENFORCE_SCHEMA] = "true"
        props[Neo4jSourceConnectorConfig.SOURCE_TYPE_QUERY] = """
                |RETURN {
                |   prop1: 1,
                |   prop2: "string",
                |   prop3: true,
                |   prop4: null,
                |   prop5: {
                |       prop: null
                |   },
                |   prop6: [1],
                |   prop7: [null]
                |} AS data, 1717773205 AS timestamp
            """.trimMargin()
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

        task.start(props)


        val expected = mapOf(
            "data" to mapOf(
                "prop1" to 1L,
                "prop2" to "string",
                "prop3" to true,
                "prop4" to null,
                "prop5" to mapOf("prop" to null),
                "prop6" to listOf(1L),
                "prop7" to listOf<Any?>(null)
            ), "timestamp" to 1717773205L
        )

        Assert.assertEventually(ThrowingSupplier {
            task.poll()?.map { (it.value() as Struct).toMap() }?.first()
        }, Matchers.equalTo(expected), 30, TimeUnit.SECONDS)
    }

    @Test
    fun `should convert point data`() {
        val topic = UUID.randomUUID().toString()
        val props = mutableMapOf<String, String>()
        props[Neo4jConnectorConfig.SERVER_URI] = neo4j.boltUrl
        props[Neo4jSourceConnectorConfig.TOPIC] = topic
        props[Neo4jSourceConnectorConfig.STREAMING_POLL_INTERVAL] = "1000"
        props[Neo4jSourceConnectorConfig.STREAMING_PROPERTY] = "timestamp"
        props[Neo4jSourceConnectorConfig.ENFORCE_SCHEMA] = "true"
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()
        props[Neo4jSourceConnectorConfig.SOURCE_TYPE_QUERY] = """
                MATCH (n:SourceNode)
                WHERE n.timestamp > 0
                RETURN n.cartesian2d AS cartesian2d,
                   n.cartesian3d AS cartesian3d,
                   n.geo2d AS geo2d,
                   n.geo3d AS geo3d
            """
        task.start(props)

        session.run("CREATE (n:SourceNode" +
                "{" +
                    "timestamp: timestamp(), " +
                    "cartesian2d: point({x: 56.7, y: 12.78}), " +
                    "cartesian3d: point({x: 56.7, y: 12.78, z: 8}), " +
                    "geo2d: point({longitude: 56.7, latitude: 12.78}), " +
                    "geo3d: point({longitude: 56.7, latitude: 12.78, height: 8})" +
                "})").consume()

        val expected = mapOf(
            "cartesian2d" to mapOf("crs" to "cartesian", "x" to 56.7, "y" to 12.78),
            "cartesian3d" to mapOf("crs" to "cartesian-3d", "x" to 56.7, "y" to 12.78, "z" to 8.0),
            "geo2d" to mapOf("crs" to "wgs-84", "longitude" to 56.7, "latitude" to 12.78),
            "geo3d" to mapOf("crs" to "wgs-84-3d", "longitude" to 56.7, "latitude" to 12.78, "height" to 8.0))

        Assert.assertEventually(ThrowingSupplier {
            task.poll()?.map { (it.value() as Struct).toMap() }?.first()
        }, Matchers.equalTo(expected), 30, TimeUnit.SECONDS)
    }
}