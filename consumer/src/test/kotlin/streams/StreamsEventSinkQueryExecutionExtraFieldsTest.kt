package streams

import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.NullLog
import org.neo4j.test.TestGraphDatabaseFactory
import streams.kafka.KafkaSinkConfiguration
import streams.service.StreamsSinkEntity
import streams.service.TopicType
import streams.service.Topics
import java.util.*
import kotlin.test.assertEquals

class StreamsEventSinkQueryExecutionExtraFieldsTest {
    private lateinit var db: GraphDatabaseService
    private lateinit var streamsEventSinkQueryExecution: StreamsEventSinkQueryExecution

    @Before
    fun setUp() {
        db = TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .newGraphDatabase()

    }

    @After
    fun tearDown() {
        db.shutdown()
    }

    @Test
    fun shouldWriteCypherQuery() {
        setUpWithPrefixes("__fecha__", "__cabecera__", "__clave__")

        val timestamp = Random().nextLong()
        val headerKey = "transactionId"
        val headerValue = UUID.randomUUID().toString()
        val headers = RecordHeaders().add(headerKey, headerValue.toByteArray()).map { Pair(it.key(), String(it.value())) }.toMap()
        val name = "David de los Santos Boix"
        val first = mapOf("id" to "1", "name" to name, "properties" to mapOf("a" to 1))
        streamsEventSinkQueryExecution.writeForTopic("shouldWriteCypherQuery", listOf(StreamsSinkEntity(first, first,headers, timestamp)))

        println(db.execute("MATCH (n:Label) RETURN n").resultAsString())

        db.execute("MATCH (n:Label {ts: ${timestamp}, trx: '${headerValue}', name: '${name}'}) RETURN count(n) AS count").columnAs<Long>("count").use {
            assertEquals(1, it.next())
        }

    }

    private fun setUpWithPrefixes(eventPrefixTimestamp: String, eventPrefixHeaders: String, eventPrefixKey: String) = run {
        val kafkaConfig = KafkaSinkConfiguration(streamsSinkConfiguration = StreamsSinkConfiguration(topics = Topics(cypherTopics = mapOf("shouldWriteCypherQuery" to "MERGE (n:Label {id: event.id})\n" +
            "    ON CREATE SET n += event.properties, n.ts = event.${eventPrefixTimestamp}, n.trx = event.${eventPrefixHeaders}.transactionId, n.name = event.${eventPrefixKey}.name"))))
        val streamsTopicService = StreamsTopicService(db as GraphDatabaseAPI)
        streamsTopicService.set(TopicType.CYPHER, kafkaConfig.streamsSinkConfiguration.topics.cypherTopics)
        streamsEventSinkQueryExecution = StreamsEventSinkQueryExecution(streamsTopicService, db as GraphDatabaseAPI,
                NullLog.getInstance(), emptyMap(), eventPrefixTimestamp, eventPrefixHeaders, eventPrefixKey)
    }

}