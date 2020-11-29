package integrations.kafka

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import extension.newDatabase
import org.junit.Test
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.test.TestGraphDatabaseFactory
import org.testcontainers.containers.GenericContainer
import streams.events.StreamsPluginStatus
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
    fun `the db should start even with no bootstrap servers provided`() {
        val db = TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig("kafka.bootstrap.servers", "")
                .setConfig("streams.sink.enabled", "true")
                .setConfig("streams.sink.topic.cypher.$topic", "CREATE (p:Place{name: event.name, coordinates: event.coordinates, citizens: event.citizens})")
                .newDatabase(StreamsPluginStatus.STOPPED) as GraphDatabaseAPI
        val count = db.execute("MATCH (n) RETURN COUNT(n) AS count").columnAs<Long>("count").next()
        assertEquals(0L, count)
        db.shutdown()
    }

    @Test
    fun `the db should start even with AVRO serializers and no schema registry url provided`() {
        val fakeWebServer = FakeWebServer()
        fakeWebServer.start()
        val url = fakeWebServer.getUrl().replace("http://", "")
        val db = TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig("kafka.bootstrap.servers", url)
                .setConfig("kafka.zookeeper.connect", url)
                .setConfig("streams.sink.enabled", "true")
                .setConfig("streams.sink.topic.cypher.$topic", "CREATE (p:Place{name: event.name, coordinates: event.coordinates, citizens: event.citizens})")
                .setConfig("kafka.key.deserializer", KafkaAvroDeserializer::class.java.name)
                .setConfig("kafka.value.deserializer", KafkaAvroDeserializer::class.java.name)
                .newDatabase(StreamsPluginStatus.STOPPED) as GraphDatabaseAPI
        val count = db.execute("MATCH (n) RETURN COUNT(n) AS count").columnAs<Long>("count").next()
        assertEquals(0L, count)
        fakeWebServer.stop()
        db.shutdown()
    }
}