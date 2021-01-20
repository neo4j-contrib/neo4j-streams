package streams.integrations

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.junit.Test
import org.neo4j.graphdb.QueryExecutionException
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
            assertTrue { it.hasNext() }
            val resultMap = (it.next())["value"] as Map<String, Any>
            assertNotNull(resultMap["offset"])
            assertNotNull(resultMap["partition"])
            assertNotNull(resultMap["keySize"])
            assertNotNull(resultMap["valueSize"])
            assertNotNull(resultMap["timestamp"])
            assertFalse { it.hasNext() }
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
            assertTrue { it.hasNext() }
            val resultMap = (it.next())["value"] as Map<String, Any>
            assertNotNull(resultMap["offset"])
            assertNotNull(resultMap["partition"])
            assertNotNull(resultMap["keySize"])
            assertNotNull(resultMap["valueSize"])
            assertNotNull(resultMap["timestamp"])
            assertFalse { it.hasNext() }
        }

        val records = kafkaConsumer.poll(5000)
        assertEquals(1, records.count())
        assertTrue { records.all {
            JSONUtils.readValue<StreamsEvent>(it.value()).payload == message
        }}
    }

    @Test
    fun testProcedureSyncWithKeyNull() {
        setUpProcedureTests()
        db.execute("CREATE (n:Foo {id: 1, name: 'Bar'})")

        val recordsCreation = kafkaConsumer.poll(5000)
        assertEquals(1, recordsCreation.count())

        val message = "Hello World"
            db.execute("MATCH (n:Foo {id: 1}) CALL streams.publish.sync('neo4j', '$message', {key: n.foo}) YIELD value RETURN value") {
            assertTrue { it.hasNext() }
            val resultMap = (it.next())["value"] as Map<String, Any>
            assertNotNull(resultMap["offset"])
            assertNotNull(resultMap["partition"])
            assertNotNull(resultMap["keySize"])
            assertNotNull(resultMap["valueSize"])
            assertNotNull(resultMap["timestamp"])
            assertFalse { it.hasNext() }
        }

        val records = kafkaConsumer.poll(5000)
        assertEquals(1, records.count())
        assertTrue { records.all {
            JSONUtils.readValue<StreamsEvent>(it.value()).payload == message
                    && it.key() == null
        }}
    }

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
                assertTrue { it.hasNext() }
                val resultMap = (it.next())["value"] as Map<String, Any>
                assertNotNull(resultMap["offset"])
                assertEquals(partitionRecord, resultMap["partition"])
                assertNotNull(resultMap["keySize"])
                assertNotNull(resultMap["valueSize"])
                assertNotNull(resultMap["timestamp"])
                assertFalse { it.hasNext() }
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
}