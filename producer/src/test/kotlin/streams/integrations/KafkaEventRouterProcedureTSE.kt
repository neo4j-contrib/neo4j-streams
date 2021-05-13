package streams.integrations

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.junit.Test
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.graphdb.Result
import streams.events.StreamsEvent
import streams.extensions.execute
import streams.utils.JSONUtils
import streams.start
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.assertNotNull

class KafkaEventRouterProcedureTSE : KafkaEventRouterBaseTSE() {

    @Test
    fun testProcedure() {
        db.start()
        val topic = UUID.randomUUID().toString()
        KafkaEventRouterSuiteIT.registerPublishProcedure(db)
        kafkaConsumer.subscribe(listOf(topic))
        val message = "Hello World"
        db.execute("CALL streams.publish('$topic', '$message')")
        val records = kafkaConsumer.poll(5000)
        assertEquals(1, records.count())
        assertTrue { records.all {
            JSONUtils.readValue<StreamsEvent>(it.value()).let {
                message == it.payload
            }
        }}
    }

    @Test
    fun testProcedureWithKey() {
        db.start()
        val topic = UUID.randomUUID().toString()
        KafkaEventRouterSuiteIT.registerPublishProcedure(db)
        kafkaConsumer.subscribe(listOf(topic))
        val message = "Hello World"
        val keyRecord = "test"
        db.execute("CALL streams.publish('$topic', '$message', {key: '$keyRecord'} )")
        val records = kafkaConsumer.poll(5000)
        assertEquals(1, records.count())
        assertTrue { records.all {
            JSONUtils.readValue<StreamsEvent>(it.value()).payload == message
            && JSONUtils.readValue<String>(it.key()) == keyRecord
        }}
    }

    @Test
    fun testProcedureWithKeyAsMap() {
        db.start()
        val topic = UUID.randomUUID().toString()
        KafkaEventRouterSuiteIT.registerPublishProcedure(db)
        kafkaConsumer.subscribe(listOf(topic))
        val message = "Hello World"
        val keyRecord = mapOf("one" to "Foo", "two" to "Baz", "three" to "Bar")
        db.execute("CALL streams.publish('$topic', '$message', {key: \$key } )", mapOf("key" to keyRecord))
        val records = kafkaConsumer.poll(5000)
        assertEquals(1, records.count())
        assertTrue { records.all {
            JSONUtils.readValue<StreamsEvent>(it.value()).payload == message
            && JSONUtils.readValue<Map<String, String>>(it.key()) == keyRecord
        }}
    }

    @Test
    fun testProcedureWithPartitionAsNotNumber() {
        db.start()
        val topic = UUID.randomUUID().toString()
        KafkaEventRouterSuiteIT.registerPublishProcedure(db)
        kafkaConsumer.subscribe(listOf(topic))
        val message = "Hello World"
        val keyRecord = "test"
        val partitionRecord = "notNumber"
        assertFailsWith(QueryExecutionException::class) {
            db.execute("CALL streams.publish('$topic', '$message', {key: '$keyRecord', partition: '$partitionRecord' })")
        }
    }

    @Test
    fun testProcedureWithPartitionAndKey() {
        db.start()
        val topic = UUID.randomUUID().toString()
        KafkaEventRouterSuiteIT.registerPublishProcedure(db)
        kafkaConsumer.subscribe(listOf(topic))
        val message = "Hello World"
        val keyRecord = "test"
        val partitionRecord = 0
        db.execute("CALL streams.publish('$topic', '$message', {key: '$keyRecord', partition: $partitionRecord })")
        val records = kafkaConsumer.poll(5000)
        assertEquals(1, records.count())
        assertTrue{ records.all {
            JSONUtils.readValue<StreamsEvent>(it.value()).payload == message
            && JSONUtils.readValue<String>(it.key()) == keyRecord
            && partitionRecord == it.partition()
        }}
    }

    @Test
    fun testCantPublishNull() {
        setUpProcedureTests()
        assertFailsWith(RuntimeException::class) {
            db.execute("CALL streams.publish('neo4j', null)")
        }
    }

    @Test
    fun testProcedureSyncWithNode() {
        setUpProcedureTests()
        db.execute("CREATE (n:Baz {age: 23, name: 'Foo', surname: 'Bar'})")

        val recordsCreation = kafkaConsumer.poll(5000)
        assertEquals(1, recordsCreation.count())

        db.execute("MATCH (n:Baz) \n" +
                "CALL streams.publish.sync('neo4j', n) \n" +
                "YIELD value \n" +
                "RETURN value") {
            publishSyncAssertions(it)
        }

        val records = kafkaConsumer.poll(5000)
        assertEquals(1, records.count())
        assertEquals(3, ((records.map {
            JSONUtils.readValue<StreamsEvent>(it.value()).payload
        }[0] as Map<String, Any>)["properties"] as Map<String, Any>).size)
    }

    @Test
    fun testProcedureSync() {
        setUpProcedureTests()
        val message = "Hello World"
        db.execute("CALL streams.publish.sync('neo4j', '$message')") {
            publishSyncAssertions(it)
        }

        val records = kafkaConsumer.poll(5000)
        assertEquals(1, records.count())
        assertTrue { records.all {
            JSONUtils.readValue<StreamsEvent>(it.value()).payload == message
        }}
    }


    @Test
    fun testProcedureWithRelationship() {
        setUpProcedureTests()
        db.execute("CREATE (:Foo {one: 'two'})-[:KNOWS {alpha: 'beta'}]->(:Bar {three: 'four'})")

        val recordsCreation = kafkaConsumer.poll(5000)
        assertEquals(3, recordsCreation.count())

        db.execute("""
            MATCH (:Foo)-[r:KNOWS]->(:Bar)
            |CALL streams.publish.sync('neo4j', r)
            |YIELD value RETURN value""".trimMargin()) {
            publishSyncAssertions(it)
        }
        val records = kafkaConsumer.poll(5000)
        assertEquals(1, records.count())
        
        val payload = JSONUtils.readValue<StreamsEvent>(records.first().value()).payload as Map<String, Any>
        assertTrue(payload["id"] is String)
        assertEquals(mapOf("alpha" to "beta"), payload["properties"])
        assertEquals("KNOWS", payload["label"])
        assertEquals("relationship", payload["type"])
        val start = payload["start"] as Map<String, Any>
        assertEquals(listOf("Foo"), start["labels"])
        assertEquals(mapOf("one" to "two"), start["properties"])
        assertEquals("node", start["type"])
        val end = payload["end"] as Map<String, Any>
        assertEquals(listOf("Bar"), end["labels"])
        assertEquals(mapOf("three" to "four"), end["properties"])
        assertEquals("node", end["type"])
    }

    @Test
    fun testProcedureSyncWithKeyNull() {
        setUpProcedureTests()
        db.execute("CREATE (n:Foo {id: 1, name: 'Bar'})")

        val recordsCreation = kafkaConsumer.poll(5000)
        assertEquals(1, recordsCreation.count())

        val message = "Hello World"
        db.execute("MATCH (n:Foo {id: 1}) CALL streams.publish.sync('neo4j', '$message', {key: n.foo}) YIELD value RETURN value") { 
            publishSyncAssertions(it)
        }

        val records = kafkaConsumer.poll(5000)
        assertEquals(1, records.count())
        assertTrue { records.all {
            JSONUtils.readValue<StreamsEvent>(it.value()).payload == message
                    && it.key() == null
        }}
    }

    @Test
    fun testProcedureSyncWithConfig() {
        db.start()
        AdminClient.create(mapOf("bootstrap.servers" to KafkaEventRouterSuiteIT.kafka.bootstrapServers)).use {
            val topic = UUID.randomUUID().toString()

            it.createTopics(listOf(NewTopic(topic, 5, 1)))
                    .all()
                    .get()
            KafkaEventRouterSuiteIT.registerPublishProcedure(db)
            kafkaConsumer.subscribe(listOf(topic))

            val message = "Hello World"
            val keyRecord = "test"
            val partitionRecord = 1
            db.execute("CALL streams.publish.sync('$topic', '$message', {key: '$keyRecord', partition: $partitionRecord })") {
                publishSyncAssertions(it)
            }

            val records = kafkaConsumer.poll(5000)
            assertEquals(1, records.count())
            assertEquals(1, records.count { it.partition() == 1 })
            assertTrue{ records.all {
                JSONUtils.readValue<StreamsEvent>(it.value()).payload == message
                && JSONUtils.readValue<String>(it.key()) == keyRecord
                && partitionRecord == it.partition()
            }}
        }
    }

    @Test
    fun testProcedureWithTopicWithMultiplePartitionAndKey() {
        db.start()
        AdminClient.create(mapOf("bootstrap.servers" to KafkaEventRouterSuiteIT.kafka.bootstrapServers)).use {
            val topic = UUID.randomUUID().toString()

            it.createTopics(listOf(NewTopic(topic, 3, 1)))
                    .all()
                    .get()
            KafkaEventRouterSuiteIT.registerPublishProcedure(db)
            kafkaConsumer.subscribe(listOf(topic))

            val message = "Hello World"
            val keyRecord = "test"
            val partitionRecord = 2
            db.execute("CALL streams.publish('$topic', '$message', {key: '$keyRecord', partition: $partitionRecord })")

            val records = kafkaConsumer.poll(5000)
            assertEquals(1, records.count())
            assertEquals(1, records.count { it.partition() == 2 })
            assertTrue{ records.all {
                JSONUtils.readValue<StreamsEvent>(it.value()).payload == message
                && JSONUtils.readValue<String>(it.key()) == keyRecord
                && partitionRecord == it.partition()
            }}
        }
    }

    @Test
    fun testProcedureSendMessageToNotExistentPartition() {
        db.start()
        AdminClient.create(mapOf("bootstrap.servers" to KafkaEventRouterSuiteIT.kafka.bootstrapServers)).use {
            val topic = UUID.randomUUID().toString()

            it.createTopics(listOf(NewTopic(topic, 3, 1)))
                    .all()
                    .get()
            KafkaEventRouterSuiteIT.registerPublishProcedure(db)
            kafkaConsumer.subscribe(listOf(topic))

            val message = "Hello World"
            val keyRecord = "test"
            val partitionRecord = 9
            db.execute("CALL streams.publish('$topic', '$message', {key: '$keyRecord', partition: $partitionRecord })")

            val records = kafkaConsumer.poll(5000)
            assertEquals(0, records.count())
        }
    }

    private fun setUpProcedureTests() {
        db.start()
        KafkaEventRouterSuiteIT.registerPublishProcedure(db)
        kafkaConsumer.subscribe(listOf("neo4j"))
    }

    private fun publishSyncAssertions(it: Result) {
        assertTrue { it.hasNext() }
        val resultMap = (it.next())["value"] as Map<String, Any>
        assertNotNull(resultMap["offset"])
        assertNotNull(resultMap["partition"])
        assertNotNull(resultMap["keySize"])
        assertNotNull(resultMap["valueSize"])
        assertNotNull(resultMap["timestamp"])
        assertFalse { it.hasNext() }
    }
}