package streams.integrations

import org.junit.Test
import streams.extensions.execute
import streams.setConfig
import streams.start
import kotlin.test.assertEquals

class KafkaEventRouterLogCompactionTSE: KafkaEventRouterLogCompactionBaseTSE() {

    @Test
    fun testLogCompactionEnabled() {
        db.setConfig("streams.source.topic.nodes.compact", "Person{name,surname}")
        db.setConfig("streams.source.schema.polling.interval", "0")
        db.start()
        kafkaConsumer.subscribe(listOf("compact"))

        db.execute("CREATE (:Person {name: 'Name', surname: 'Surname'})")

        val records = kafkaConsumer.poll(5000L)

        val count = db.execute("MATCH (n) RETURN COUNT(n) AS count") { it.columnAs<Long>("count").next() }
        assertEquals(1, count)


        assertEquals(1, records.count())
        assertEquals("0", records?.first()?.key())
    }
}