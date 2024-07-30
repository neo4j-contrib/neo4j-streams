package streams.kafka.connect.sink

import com.fasterxml.jackson.databind.ObjectMapper
import com.github.jcustenborder.kafka.connect.utils.config.Description
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationNote
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationTip
import com.github.jcustenborder.kafka.connect.utils.config.TaskConfigs
import com.github.jcustenborder.kafka.connect.utils.config.Title
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import streams.kafka.connect.common.ConfigurationMigrator
import streams.kafka.connect.utils.PropertiesUtil

@Title("Neo4j Sink Connector")
@Description("The Neo4j Sink connector reads data from Kafka and and writes the data to Neo4j using a Cypher Template")
@DocumentationTip("If you need to control the size of transaction that is submitted to Neo4j you try adjusting the ``consumer.max.poll.records`` setting in the worker.properties for Kafka Connect.")
@DocumentationNote("For each topic you can provide a Cypher Template by using the following syntax ``neo4j.topic.cypher.<topic_name>=<cypher_query>``")
class Neo4jSinkConnector: SinkConnector() {
    private val log: Logger = LoggerFactory.getLogger(Neo4jSinkConnector::class.java)
    private lateinit var settings: Map<String, String>
    private lateinit var config: Neo4jSinkConnectorConfig

    override fun taskConfigs(maxTasks: Int): MutableList<MutableMap<String, String>> {
        return TaskConfigs.multiple(settings, maxTasks)
    }

    override fun start(props: MutableMap<String, String>?) {
        settings = props!!
        config = Neo4jSinkConnectorConfig(settings)
    }

    override fun stop() {
        val migratedConfig = ConfigurationMigrator(settings).migrate()
        val mapper = ObjectMapper()
        val jsonConfig = mapper.writeValueAsString(migratedConfig)
        log.info("Migrated Sink configuration to v5.1 connector format: {}", jsonConfig)
    }

    override fun version(): String {
        return PropertiesUtil.getVersion()
    }

    override fun taskClass(): Class<out Task> {
        return Neo4jSinkTask::class.java
    }

    override fun config(): ConfigDef {
        return Neo4jSinkConnectorConfig.config()
    }

}