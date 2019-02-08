package streams.kafka.connect.sink

import com.github.jcustenborder.kafka.connect.utils.VersionUtil
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import streams.utils.StreamsUtils


class Neo4jSinkTask : SinkTask() {
    private val log: Logger = LoggerFactory.getLogger(Neo4jSinkTask::class.java)
    private lateinit var config: Neo4jSinkConnectorConfig
    private lateinit var neo4jService: Neo4jService

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

        val data = EventBuilder()
                .withBatchSize(config.batchSize)
                .withTopics(config.getAllTopics())
                .withSinkRecords(collection)
                .build()
        neo4jService.writeData(data)
    }

    override fun stop() {
        log.info("Stop() - Closing driver manager.")
        StreamsUtils.ignoreExceptions({ neo4jService.close() }, UninitializedPropertyAccessException::class.java)
    }
}