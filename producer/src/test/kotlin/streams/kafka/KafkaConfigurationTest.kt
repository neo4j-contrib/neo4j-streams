package streams.kafka

import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class KafkaConfigurationTest {

    @Test
    fun shouldCreateConfiguration() {
        val map = mapOf("kafka.zookeeper.connect" to "zookeeper:1234",
                "kafka.bootstrap.servers" to "kafka:5678",
                "kafka.acks" to "10",
                "kafka.retries" to 1,
                "kafka.batch.size" to 10,
                "kafka.buffer.memory" to 1000,
                "kafka.reindex.batch.size" to 1,
                "kafka.session.timeout.ms" to 1,
                "kafka.connection.timeout.ms" to 1,
                "kafka.replication" to 2,
                "kafka.transactional.id" to "foo",
                "kafka.linger.ms" to 10,
                "kafka.fetch.min.bytes" to 1234,
                "kafka.topic.discovery.polling.interval" to 0L,
                "kafka.streams.log.compaction.strategy" to "delete")

        val kafkaConfig = KafkaConfiguration.create(map.mapValues { it.value.toString() })

        assertFalse { kafkaConfig.extraProperties.isEmpty() }
        assertTrue { kafkaConfig.extraProperties.containsKey("fetch.min.bytes") }
        assertEquals(1,  kafkaConfig.extraProperties.size)

        val properties = kafkaConfig.asProperties()

        assertEquals(map["kafka.zookeeper.connect"], properties["zookeeper.connect"])
        assertEquals(map["kafka.bootstrap.servers"], properties["bootstrap.servers"])
        assertEquals(map["kafka.acks"], properties["acks"])
        assertEquals(map["kafka.retries"], properties["retries"])
        assertEquals(map["kafka.batch.size"], properties["batch.size"])
        assertEquals(map["kafka.buffer.memory"], properties["buffer.memory"])
        assertEquals(map["kafka.reindex.batch.size"], properties["reindex.batch.size"])
        assertEquals(map["kafka.session.timeout.ms"], properties["session.timeout.ms"])
        assertEquals(map["kafka.connection.timeout.ms"], properties["connection.timeout.ms"])
        assertEquals(map["kafka.replication"], properties["replication"])
        assertEquals(map["kafka.transactional.id"], properties["transactional.id"])
        assertEquals(map["kafka.linger.ms"], properties["linger.ms"])
        assertEquals(map["kafka.fetch.min.bytes"].toString(), properties["fetch.min.bytes"])
        assertEquals(map["kafka.topic.discovery.polling.interval"], properties["topic.discovery.polling.interval"])
        assertEquals(map["kafka.streams.log.compaction.strategy"], properties["streams.log.compaction.strategy"])
    }
}