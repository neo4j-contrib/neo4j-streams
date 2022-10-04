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
import org.neo4j.driver.types.Node
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
        props[Neo4jSourceConnectorConfig.STREAMING_POLL_INTERVAL] = "10"
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
        props[Neo4jSourceConnectorConfig.STREAMING_POLL_INTERVAL] = "10"
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
        props[Neo4jSourceConnectorConfig.STREAMING_POLL_INTERVAL] = "10"
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
        props[Neo4jSourceConnectorConfig.STREAMING_POLL_INTERVAL] = "10"
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
            val result = tx.run("""
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
                            """.trimMargin())
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
        props[Neo4jSourceConnectorConfig.STREAMING_POLL_INTERVAL] = "10"
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
        props[Neo4jSourceConnectorConfig.STREAMING_POLL_INTERVAL] = "10"
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
        props[Neo4jSourceConnectorConfig.STREAMING_POLL_INTERVAL] = "10"
        props[Neo4jSourceConnectorConfig.SOURCE_TYPE_QUERY] = "WRONG QUERY".trimMargin()
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

        task.start(props)
        val totalRecords = 10
        insertRecords(totalRecords)

        task.poll()
    }
}