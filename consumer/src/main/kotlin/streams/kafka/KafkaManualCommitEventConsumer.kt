package streams.kafka

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.neo4j.logging.Log
import streams.service.StreamsSinkEntity
import streams.service.errors.ErrorService

class KafkaManualCommitEventConsumer(private val config: KafkaSinkConfiguration,
                                     private val log: Log,
                                     private val dlqService: ErrorService): KafkaAutoCommitEventConsumer(config, log, dlqService) {

    private fun commitData(commit: Boolean, topicMap: Map<TopicPartition, OffsetAndMetadata>) {
        if (commit) {
            consumer.commitSync(topicMap)
        }
    }

    override fun read(action: (String, List<StreamsSinkEntity>) -> Unit) {
        val topicMap = readSimple(action)
        commitData(true, topicMap)
    }

    override fun read(topicConfig: Map<String, Any>, action: (String, List<StreamsSinkEntity>) -> Unit) {
        val kafkaTopicConfig = KafkaTopicConfig.fromMap(topicConfig)
        val topicMap = if (kafkaTopicConfig.topicPartitionsMap.isEmpty()) {
            readSimple(action)
        } else {
            readFromPartition(kafkaTopicConfig, action)
        }
        commitData(kafkaTopicConfig.commit, topicMap)
    }
}