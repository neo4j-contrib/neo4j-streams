package streams.config

import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import org.neo4j.logging.Log
import streams.extensions.getDefaultDbName
import java.io.FileInputStream
import java.io.FileNotFoundException
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

data class StreamsConfig(private val log: Log, private val dbms: DatabaseManagementService) : LifecycleAdapter() {

    val config = ConcurrentHashMap<String, String>()

    private lateinit var neo4jConfFolder: String

    companion object {
        private lateinit var INSTANCE: StreamsConfig
        private val SUPPORTED_PREFIXES = listOf("streams", "kafka")
        private const val SUN_JAVA_COMMAND = "sun.java.command"
        private const val CONF_DIR_ARG = "config-dir="
        const val SOURCE_ENABLED = "streams.source.enabled"
        const val SOURCE_ENABLED_VALUE = true
        const val PROCEDURES_ENABLED = "streams.procedures.enabled"
        const val PROCEDURES_ENABLED_VALUE = true
        const val SINK_ENABLED = "streams.sink.enabled"
        const val SINK_ENABLED_VALUE = false
        const val DEFAULT_PATH = "."
        const val CHECK_APOC_TIMEOUT = "streams.check.apoc.timeout"
        const val CHECK_APOC_INTERVAL = "streams.check.apoc.interval"
        const val CLUSTER_ONLY = "streams.cluster.only"
        const val CHECK_WRITEABLE_INSTANCE_INTERVAL = "streams.check.writeable.instance.interval"
        const val SYSTEM_DB_WAIT_TIMEOUT = "streams.systemdb.wait.timeout"
        const val SYSTEM_DB_WAIT_TIMEOUT_VALUE = 10000L
        private var afterInitListeners = mutableListOf<((MutableMap<String, String>) -> Unit)>()

        fun registerListener(after: (MutableMap<String, String>) -> Unit) {
            afterInitListeners.add(after)
        }

        fun getInstance() = INSTANCE
    }

    override fun init() {
        if (log.isDebugEnabled) {
            log.debug("Init StreamsConfig...")
        }
        neo4jConfFolder = getNeo4jConfFolder()
        loadConfiguration()
        afterInitListeners.forEach { it(config) }
        INSTANCE = this
    }

    override fun stop() {
        afterInitListeners.clear()
    }

    private fun loadConfiguration() {
        val properties = neo4jConfAsProperties()

        val filteredValues = filterProperties(properties)
                { key -> !SUPPORTED_PREFIXES.find { key.toString().startsWith(it) }.isNullOrBlank() }

        if (log.isDebugEnabled) {
            log.debug("Neo4j Streams Global configuration from neo4j.conf file: $filteredValues")
        }

        config.putAll(filteredValues)
    }

    private fun filterProperties(properties: Properties, filter: (Any) -> Boolean) = properties
            .filterKeys(filter)
            .mapNotNull {
                if (it.value == null) {
                    null
                } else {
                    it.key.toString() to it.value.toString()
                }
            }
            .toMap()

    fun loadStreamsConfiguration() {
        val properties = neo4jConfAsProperties()

        val filteredValues = filterProperties(properties)
            { key -> key.toString().startsWith("streams.") }

        if (log.isDebugEnabled) {
            log.debug("Neo4j Streams configuration reloaded from neo4j.conf file: $filteredValues")
        }

        config.putAll(filteredValues)
    }

    private fun neo4jConfAsProperties(): Properties {
        val neo4jConfFolder = System.getenv().getOrDefault("NEO4J_CONF", neo4jConfFolder)

        val properties = Properties()
        try {
            log.info("The retrieved NEO4J_CONF dir is $neo4jConfFolder")
            properties.load(FileInputStream("$neo4jConfFolder/neo4j.conf"))
        } catch (e: FileNotFoundException) {
            log.error("The neo4j.conf file is not under the directory defined into the directory $neo4jConfFolder, please set the NEO4J_CONF env correctly")
        }
        return properties
    }

    // Taken from ApocConfig.java
    private fun getNeo4jConfFolder(): String { // sun.java.command=com.neo4j.server.enterprise.CommercialEntryPoint --home-dir=/home/myid/neo4j-enterprise-4.0.0-alpha09mr02 --config-dir=/home/myid/neo4j-enterprise-4.0.0-alpha09mr02/conf
        val command = System.getProperty(SUN_JAVA_COMMAND, "")
        val neo4jConfFolder = command.split("--")
                .map(String::trim)
                .filter { it.startsWith(CONF_DIR_ARG) }
                .map { it.substring(CONF_DIR_ARG.length) }
                .firstOrNull() ?: DEFAULT_PATH

        if (neo4jConfFolder == DEFAULT_PATH) {
            log.info("Cannot determine conf folder from sys property $command, assuming $neo4jConfFolder")
        } else {
            log.info("From system properties: NEO4J_CONF=%s", neo4jConfFolder)
        }
        return neo4jConfFolder
    }

    fun defaultDbName() = this.dbms.getDefaultDbName()

    fun isDefaultDb(dbName: String) = this.defaultDbName() == dbName

    fun isSourceGloballyEnabled() = this.config.getOrDefault(SOURCE_ENABLED, SOURCE_ENABLED_VALUE).toString().toBoolean()

    fun isSourceEnabled(dbName: String) = this.config.getOrDefault("${SOURCE_ENABLED}.from.$dbName", isSourceGloballyEnabled()).toString().toBoolean()

    fun hasProceduresGloballyEnabled() = this.config.getOrDefault(PROCEDURES_ENABLED, PROCEDURES_ENABLED_VALUE).toString().toBoolean()

    fun hasProceduresEnabled(dbName: String) = this.config.getOrDefault("${PROCEDURES_ENABLED}.$dbName", hasProceduresGloballyEnabled()).toString().toBoolean()

    fun isSinkGloballyEnabled() = this.config.getOrDefault(SINK_ENABLED, SINK_ENABLED_VALUE).toString().toBoolean()

    fun isSinkEnabled(dbName: String) = this.config.getOrDefault("${SINK_ENABLED}.to.$dbName", isSinkGloballyEnabled()).toString().toBoolean()

    fun getSystemDbWaitTimeout() = this.config.getOrDefault(SYSTEM_DB_WAIT_TIMEOUT, SYSTEM_DB_WAIT_TIMEOUT_VALUE)
            .toString().toLong()

}