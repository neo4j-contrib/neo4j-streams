package streams.kafka

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.admin.AdminClient
import streams.utils.KafkaValidationUtils
import streams.utils.StreamsUtils
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

class KafkaAdminService(private val props: KafkaConfiguration, private val allTopics: List<String>) {
    private val client = AdminClient.create(props.asProperties())
    private val kafkaTopics: MutableSet<String> = Collections.newSetFromMap(ConcurrentHashMap<String, Boolean>())
    private val isAutoCreateTopicsEnabled = KafkaValidationUtils.isAutoCreateTopicsEnabled(client)
    private lateinit var job: Job

    fun start() {
        if (!isAutoCreateTopicsEnabled) {
            job = GlobalScope.launch(Dispatchers.IO) {
                while (isActive) {
                    kafkaTopics += client.listTopics().names().get()
                    delay(props.topicDiscoveryPollingInterval)
                }
            }
        }
    }

    fun stop() {
        StreamsUtils.ignoreExceptions({
            runBlocking {
                job.cancelAndJoin()
            }
        }, UninitializedPropertyAccessException::class.java)
    }

    fun isValidTopic(topic: String) = when (isAutoCreateTopicsEnabled) {
        true -> true
        else -> kafkaTopics.contains(topic)
    }

    fun getInvalidTopics() = KafkaValidationUtils.getInvalidTopics(client, allTopics)
}