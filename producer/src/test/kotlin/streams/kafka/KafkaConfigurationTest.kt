package streams.kafka

import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class KafkaConfigurationTest {

    @Test
    fun shouldCreateConfiguration() {
        val map = mapOf("kafka.zookeeper.connect" to "zookeeper:1234",
                "kafka.bootstrap.servers" to "kafka:5678",
                "kafka.acks" to "10",
                "kafka.num.partitions" to 1,
                "kafka.retries" to 1,
                "kafka.batch.size" to 10,
                "kafka.buffer.memory" to 1000,
                "kafka.reindex.batch.size" to 1,
                "kafka.session.timeout.ms" to 1,
                "kafka.connection.timeout.ms" to 1,
                "kafka.replication" to 2,
                "kafka.transactional.id" to "foo",
                "kafka.linger.ms" to 10)

        val properties = KafkaConfiguration.from(map.mapValues { it.value.toString() }).asProperties()

        assertEquals(map["kafka.zookeeper.connect"], properties["zookeeper.connect"])
        assertEquals(map["kafka.bootstrap.servers"], properties["bootstrap.servers"])
        assertEquals(map["kafka.acks"], properties["acks"])
        assertEquals(map["kafka.num.partitions"], properties["num.partitions"])
        assertEquals(map["kafka.retries"], properties["retries"])
        assertEquals(map["kafka.batch.size"], properties["batch.size"])
        assertEquals(map["kafka.buffer.memory"], properties["buffer.memory"])
        assertEquals(map["kafka.reindex.batch.size"], properties["reindex.batch.size"])
        assertEquals(map["kafka.session.timeout.ms"], properties["session.timeout.ms"])
        assertEquals(map["kafka.connection.timeout.ms"], properties["connection.timeout.ms"])
        assertEquals(map["kafka.replication"], properties["replication"])
        assertEquals(map["kafka.transactional.id"], properties["transactional.id"])
        assertEquals(map["kafka.linger.ms"], properties["linger.ms"])
    }
}