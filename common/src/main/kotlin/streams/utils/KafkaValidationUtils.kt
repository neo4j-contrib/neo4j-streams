package streams.utils

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.config.ConfigResource
import java.util.Properties

object KafkaValidationUtils {
    fun getInvalidTopicsError(invalidTopics: List<String>) = "The BROKER config `auto.create.topics.enable` is false, the following topics need to be created into the Kafka cluster otherwise the messages will be discarded: $invalidTopics"

    fun getInvalidTopics(kafkaProps: Properties, allTopics: List<String>): List<String> = try {
        getInvalidTopics(AdminClient.create(kafkaProps), allTopics)
    } catch (e: Exception) {
        emptyList()
    }

    fun getInvalidTopics(client: AdminClient, allTopics: List<String>): List<String> = try {
        val kafkaTopics = client.listTopics().names().get()
        val invalidTopics = allTopics.filter { !kafkaTopics.contains(it) }
        if (invalidTopics.isNotEmpty() && isAutoCreateTopicsEnabled(client)) {
            emptyList()
        } else {
            invalidTopics
        }
    } catch (e: Exception) {
        emptyList()
    }

    fun isAutoCreateTopicsEnabled(kafkaProps: Properties):Boolean = try {
        isAutoCreateTopicsEnabled(AdminClient.create(kafkaProps))
    } catch (e: Exception) {
        false
    }

    fun isAutoCreateTopicsEnabled(client: AdminClient): Boolean = try {
        val firstNodeId = client.describeCluster().nodes().get().first().id()
        val configResources = listOf(ConfigResource(ConfigResource.Type.BROKER, firstNodeId.toString()))
        val configs = client.describeConfigs(configResources).all().get()
        configs.values
                .flatMap { it.entries() }
                .find { it.name() == "auto.create.topics.enable" }
                ?.value()
                ?.toBoolean() ?: false
    } catch (e: Exception) {
        false
    }
}