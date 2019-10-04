package streams.utils

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.config.ConfigResource
import java.util.*

object KafkaValidationUtils {
    fun getInvalidTopicsError(invalidTopics: List<String>) = "The BROKER config `auto.create.topics.enable` is false, the following topics need to be created into the Kafka cluster otherwise the messages will be discarded: $invalidTopics"

    fun getInvalidTopics(kafkaProps: Properties, allTopics: List<String>) = getInvalidTopics(AdminClient.create(kafkaProps), allTopics)

    fun getInvalidTopics(client: AdminClient, allTopics: List<String>): List<String> {
        val kafkaTopics = client.listTopics().names().get()
        val invalidTopics = allTopics.filter { !kafkaTopics.contains(it) }
        return if (invalidTopics.isNotEmpty()) {
            if (isAutoCreateTopicsEnabled(client)) {
                emptyList()
            } else {
                invalidTopics
            }
        } else {
            invalidTopics
        }
    }

    fun isAutoCreateTopicsEnabled(kafkaProps: Properties) = isAutoCreateTopicsEnabled(AdminClient.create(kafkaProps))

    fun isAutoCreateTopicsEnabled(client: AdminClient): Boolean {
        val firstNodeId = client.describeCluster().nodes().get().first().id()
        val configs = client.describeConfigs(listOf(ConfigResource(ConfigResource.Type.BROKER, firstNodeId.toString()))).all().get()
        return configs.values
                .flatMap { it.entries() }
                .find { it.name() == "auto.create.topics.enable" }
                ?.value()
                ?.toBoolean() ?: false
    }
}