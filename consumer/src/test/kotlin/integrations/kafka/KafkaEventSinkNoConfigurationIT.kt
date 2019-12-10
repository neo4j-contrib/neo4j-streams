package integrations.kafka

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.junit.Test
import org.neo4j.test.rule.ImpermanentDbmsRule
import org.testcontainers.containers.GenericContainer
import streams.extensions.execute
import streams.setConfig
import kotlin.test.assertEquals


class FakeWebServer: GenericContainer<FakeWebServer>("alpine") {
    override fun start() {
        this.withCommand("/bin/sh", "-c", "while true; do { echo -e 'HTTP/1.1 200 OK'; echo ; } | nc -l -p 8000; done")
                .withExposedPorts(8000)
        super.start()
    }

    fun getUrl() = "http://localhost:${getMappedPort(8000)}"
}

class KafkaEventSinkNoConfigurationIT {

    private val topic = "no-config"

    @Test
    fun `the db should start even with no bootstrap servers provided()`() {
        val db = ImpermanentDbmsRule()
                .setConfig("kafka.bootstrap.servers", "")
                .setConfig("streams.sink.enabled", "true")
                .setConfig("streams.sink.topic.cypher.$topic", "CREATE (p:Place{name: event.name, coordinates: event.coordinates, citizens: event.citizens})") as ImpermanentDbmsRule
        val count = db.execute("MATCH (n) RETURN COUNT(n) AS count").columnAs<Long>("count").next()
        assertEquals(0L, count)
    }

    @Test
    fun `the db should start even with AVRO serializers and no schema registry url provided`() {
        val fakeWebServer = FakeWebServer()
        fakeWebServer.start()
        val url = fakeWebServer.getUrl().replace("http://", "")
        val db = ImpermanentDbmsRule()
                .setConfig("kafka.bootstrap.servers", url)
                .setConfig("kafka.zookeeper.connect", url)
                .setConfig("streams.sink.enabled", "true")
                .setConfig("streams.sink.topic.cypher.$topic", "CREATE (p:Place{name: event.name, coordinates: event.coordinates, citizens: event.citizens})")
                .setConfig("kafka.key.deserializer", KafkaAvroDeserializer::class.java.name)
                .setConfig("kafka.value.deserializer", KafkaAvroDeserializer::class.java.name) as ImpermanentDbmsRule
        val count = db.execute("MATCH (n) RETURN COUNT(n) AS count").columnAs<Long>("count").next()
        assertEquals(0L, count)
        fakeWebServer.stop()
    }
}