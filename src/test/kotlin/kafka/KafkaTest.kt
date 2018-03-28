package kafka

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.codehaus.jackson.map.ObjectMapper
import org.junit.After
import org.junit.Before
import org.junit.ClassRule
import org.junit.Test
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.test.TestGraphDatabaseFactory
import org.springframework.kafka.test.rule.KafkaEmbedded
import kotlin.test.assertEquals


/**
 * @author mh
 * @since 19.03.18
 */
class KafkaTest {
    var db: GraphDatabaseService? = null
    val mapper = ObjectMapper()

    companion object {
        @ClassRule @JvmField
        var embeddedKafka = KafkaEmbedded(1, true, 1, "neo4j")
    }

    @Before
    fun setUp() {
        db = TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig("kafka.bootstrap.servers", System.getProperty("spring.embedded.kafka.brokers"))
                .newGraphDatabase()
    }

    @After
    fun tearDown() {
        db?.shutdown()
    }

    @Test
    fun createNodes() {
        val config = KafkaConfiguration(kafkaHosts = System.getProperty("spring.embedded.kafka.brokers"))
        val props = config.asProperties()
        props.put("enable.auto.commit","true");
        val consumer = KafkaConsumer<Long,ByteArray>(props)
        consumer.subscribe(config.topics)
        Thread{
            db!!.execute("CREATE (:Person {name:'John Doe', age:42})").close()
        }.start()
        val records = consumer.poll(5000)
        records.forEach { println("offset = ${it.offset()}, key = ${it.key()}, value = ${mapper.readValue(it.value(),Object::class.java)}") }
        assertEquals(1, records.count())
        assertEquals(true, records.all { mapper.readValue(it.value(),Map::class.java).let {
            it["labels"] == listOf("Person") && it["data"] == mapOf("name" to "John Doe", "age" to 42) && it["state"] == "created" } })

        // todo update, delete
    }
}
