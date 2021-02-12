package streams.configuration

import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import org.neo4j.logging.internal.LogService
import org.neo4j.plugin.configuration.ConfigurationLifecycle
import org.neo4j.plugin.configuration.ConfigurationLifecycleUtils
import org.neo4j.plugin.configuration.EventType
import org.neo4j.plugin.configuration.listners.ConfigurationLifecycleListener
import streams.utils.StreamsUtils
import java.io.File
import java.util.concurrent.ConcurrentHashMap

class StreamsConfig(triggerPeriod: Int = DEFAULT_TRIGGER_PERIOD, log: Log) {

    companion object {
        @JvmStatic private val cache = ConcurrentHashMap<String, StreamsConfig>()

        private const val DEFAULT_TRIGGER_PERIOD: Int = 10000

        private const val SUN_JAVA_COMMAND = "sun.java.command"
        private const val CONF_DIR_ARG = "config-dir="
        private const val DEFAULT_PATH = "."

        fun getNeo4jConfFolder(): String { // sun.java.command=com.neo4j.server.enterprise.CommercialEntryPoint --home-dir=/home/myid/neo4j-enterprise-4.0.0-alpha09mr02 --config-dir=/home/myid/neo4j-enterprise-4.0.0-alpha09mr02/conf
            val command = System.getProperty(SUN_JAVA_COMMAND, "")
            return command.split("--")
                    .map(String::trim)
                    .filter { it.startsWith(CONF_DIR_ARG) }
                    .map { it.substring(CONF_DIR_ARG.length) }
                    .firstOrNull() ?: DEFAULT_PATH
        }

        fun getInstance(db: GraphDatabaseAPI): StreamsConfig = cache.computeIfAbsent(StreamsUtils.getName(db)) {
            StreamsConfig(log = db.dependencyResolver
                    .resolveDependency(LogService::class.java)
                    .getUserLog(StreamsConfig::class.java))
        }

        fun removeInstance(db: GraphDatabaseAPI): StreamsConfig? = cache.remove(StreamsUtils.getName(db))
    }

    private val configLifecycle: ConfigurationLifecycle

    init {
        val neo4jConfFolder = System.getenv().getOrDefault("NEO4J_CONF", getNeo4jConfFolder())
        configLifecycle = ConfigurationLifecycle(triggerPeriod,
                "$neo4jConfFolder${File.separator}streams.conf",
                true, log, true, "streams.", "kafka.")
    }

    fun setProperty(key: String, value: Any, save: Boolean = true) {
        configLifecycle.setProperty(key, value, save)
    }

    fun setProperties(map: Map<String, Any>, save: Boolean = true) {
        configLifecycle.setProperties(map, save)
    }

    fun removeProperty(key: String, save: Boolean = true) {
        configLifecycle.removeProperty(key, save)
    }

    fun removeProperties(keys: Collection<String>, save: Boolean = true) {
        configLifecycle.removeProperties(keys, save)
    }

    fun reload() {
        configLifecycle.reload()
    }

    fun start() {
        configLifecycle.start()
    }

    fun getConfiguration(): MutableMap<String, Any> = ConfigurationLifecycleUtils.toMap(configLifecycle.configuration)

    fun addConfigurationLifecycleListener(evt: EventType,
                                          listener: ConfigurationLifecycleListener) {
        configLifecycle.addConfigurationLifecycleListener(evt, listener)
    }

    fun stop(shutdown: Boolean = false) {
        configLifecycle.stop(shutdown)
    }
}