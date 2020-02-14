package streams.integrations

import org.junit.Test
import streams.events.StreamsEvent
import streams.extensions.execute
import streams.serialization.JSONUtils
import streams.start
import kotlin.test.assertEquals

class KafkaEventRouterProcedureTSE: KafkaEventRouterBaseTSE() {

    @Test
    fun testProcedure() {
        db.start()
        KafkaEventRouterSuiteIT.registerPublishProcedure(db)
        kafkaConsumer.subscribe(listOf("neo4j"))
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

}