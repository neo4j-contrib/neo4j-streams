package streams.kafka.connect.sink

import com.github.jcustenborder.kafka.connect.utils.VersionUtil
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask
import org.slf4j.LoggerFactory
import org.slf4j.Logger
import streams.utils.StreamsUtils
import java.util.concurrent.TimeUnit
import java.util.concurrent.CountDownLatch


class Neo4jSinkTask : SinkTask() {
    private val log: Logger = LoggerFactory.getLogger(Neo4jSinkTask::class.java)
    private lateinit var config: Neo4jSinkConnectorConfig
    private lateinit var neo4jService: Neo4jService

    private val converter = ValueConverter()

    override fun version(): String {
        return VersionUtil.version(this.javaClass as Class<*>)
    }

    override fun start(map: Map<String, String>) {
        this.config = Neo4jSinkConnectorConfig(map)
        this.neo4jService = Neo4jService(this.config)
    }

    override fun put(collection: Collection<SinkRecord>) = runBlocking(Dispatchers.IO) {
        if (collection.isEmpty()) {
            return@runBlocking
        }

        val mapTopicRecords = collection.groupBy { it.topic() }

        // TODO define a retry policy in that case we must throw `RetriableException`
        val data = mapTopicRecords.filterKeys {topic ->
                    val isValidTopic = config.topicMap.containsKey(topic)
                    if (!isValidTopic && log.isDebugEnabled) {
                        log.debug("Topic $topic not present")
                    }
                    isValidTopic
                }
                .mapKeys { it -> "${StreamsUtils.UNWIND} ${config.topicMap[it.key]}" }
                .mapValues { it -> mapOf("events" to it.value.map { converter.convert(it.value()) }) }
        neo4jService.writeData(data)

    }

    override fun stop() {
        log.info("Stop() - Closing driver manager.")
        StreamsUtils.ignoreExceptions({ neo4jService.close() }, UninitializedPropertyAccessException::class.java)
    }
}