package streams.integrations

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.hamcrest.Matchers
import org.neo4j.function.ThrowingSupplier
import org.neo4j.test.assertion.Assert
import java.time.Duration
import java.util.concurrent.TimeUnit

class CompactionStrategyTestCommon {

    companion object {

        private fun compactTopic(topic: String, numTopics: Int, withCompact: Boolean) = run {
            val newTopic = NewTopic(topic, numTopics, 1)
            if (withCompact) {
                newTopic.configs(mapOf(
                        "cleanup.policy" to "compact",
                        "segment.ms" to "10",
                        "retention.ms" to "1",
                        "min.cleanable.dirty.ratio" to "0.01"))
            }
            newTopic
        }

        fun createCompactTopic(topic: String, numTopics: Int = 1, withCompact: Boolean = true) {
            AdminClient.create(mapOf("bootstrap.servers" to KafkaEventRouterBaseIT.kafka.bootstrapServers)).use {
                val topics = listOf(compactTopic(topic, numTopics, withCompact))
                it.createTopics(topics).all().get()
            }
        }

        fun assertTopicFilled(kafkaConsumer: KafkaConsumer<String, ByteArray>,
                              withSeek: Boolean = false,
                              timeout: Long = 30,
                              assertion: (ConsumerRecords<String, ByteArray>) -> Boolean = { it.count() == 1 }
        ) {
            Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
                if(withSeek) {
                    kafkaConsumer.seekToBeginning(kafkaConsumer.assignment())
                }
                val records = kafkaConsumer.poll(Duration.ofSeconds(5))
                assertion(records)
            }, Matchers.equalTo(true), timeout, TimeUnit.SECONDS)
        }
    }
}