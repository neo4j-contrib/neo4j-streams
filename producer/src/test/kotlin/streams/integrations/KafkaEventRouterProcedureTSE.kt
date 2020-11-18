package streams.integrations

import org.junit.Test
import streams.events.StreamsEvent
import streams.extensions.execute
import streams.utils.JSONUtils
import streams.start
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.assertNotNull

class KafkaEventRouterProcedureTSE : KafkaEventRouterBaseTSE() {

    @Test
    fun testProcedure() {
        setUpProcedureTests()
        val message = "Hello World"
        db.execute("CALL streams.publish('neo4j', '$message')")
        val records = kafkaConsumer.poll(5000)
        assertEquals(1, records.count())
        assertEquals(true, records.all {
            JSONUtils.readValue<StreamsEvent>(it.value()).let {
                message == it.payload
            }
        })
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
            JSONUtils.readValue<StreamsEvent>(it.value())
                    .let { it.payload }
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
        assertEquals(true, records.all {
            JSONUtils.readValue<StreamsEvent>(it.value()).let {
                message == it.payload
            }
        })
    }

    private fun setUpProcedureTests() {
        db.start()
        KafkaEventRouterSuiteIT.registerPublishProcedure(db)
        kafkaConsumer.subscribe(listOf("neo4j"))
    }
}